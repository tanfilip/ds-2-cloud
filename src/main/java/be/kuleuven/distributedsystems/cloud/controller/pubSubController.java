package be.kuleuven.distributedsystems.cloud.controller;

import be.kuleuven.distributedsystems.cloud.entities.Booking;
import be.kuleuven.distributedsystems.cloud.entities.Quote;
import be.kuleuven.distributedsystems.cloud.entities.Ticket;
import be.kuleuven.distributedsystems.cloud.exceptions.seatAlreadyBookedException;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.eclipse.jetty.util.ajax.JSON;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.annotation.Resource;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.*;

@RestController
@Component
@RequestMapping("/pubsub")
public class pubSubController {
    private static final String API_KEY = "Iw8zeveVyaPNWonPNaU0213uw3g6Ei";

    private final Gson gson = new Gson();
    private final JsonParser jsonParser = new JsonParser();

    @Resource(name="webClientBuilder")
    private WebClient.Builder webClientBuilder;

//    @Resource(name = "reliableClientBuilder")
//    WebClient.Builder reliableClientBuilder;
//
//    @Resource(name = "unreliableClientBuilder")
//    WebClient.Builder unreliableClientBuilder;

    @Resource(name = "db")
    Firestore db;

    @PostMapping("/subs")
    public ResponseEntity<String> subscription(@RequestBody String body) {
        System.out.println("\r\ngot to post /subs BEGIN\r\n");
        JsonElement jsonRoot = jsonParser.parse(body);
        JsonElement msg = jsonRoot.getAsJsonObject().get("message");
        JsonElement attributes = msg.getAsJsonObject().get("attributes");

        JsonElement quotesAsJson = attributes.getAsJsonObject().get("quotes");
        JsonElement customerAsJson = attributes.getAsJsonObject().get("customer");

        Type listQuoteType = new TypeToken<ArrayList<Quote>>() {}.getType();
        List<Quote> quotesToConfirm = gson.fromJson((String) JSON.parse(String.valueOf(quotesAsJson)), listQuoteType);
        String customer = gson.fromJson((String) JSON.parse(String.valueOf(customerAsJson)), String.class);
        String bookingReference = UUID.randomUUID().toString();
        List<Ticket> ticketsFromQuotes = changeQuotesToTickets(quotesToConfirm, customer, bookingReference);

        addBookingToDatabase(ticketsFromQuotes, customer);

        return ResponseEntity.status(HttpStatus.OK).body("OK");

    }

    private void addBookingToDatabase(List<Ticket> ticketsFromQuotes, String customer) {
        LocalDateTime currentBookingTime = LocalDateTime.now();

        CollectionReference bookings = db.collection("bookings");
        Booking newBooking = new Booking(UUID.randomUUID(),currentBookingTime, ticketsFromQuotes, customer);

        // BookingData is structured like booking => contains id, time, tickets and customers.
        Map<String, Object> bookingData = new HashMap<>();
        bookingData.put("id", newBooking.getId().toString());
        bookingData.put("time", newBooking.getTime());
        bookingData.put("tickets", newBooking.getTickets());
        bookingData.put("customer", newBooking.getCustomer());

        addTicketsToDb(newBooking);
        String bookingId = newBooking.getId().toString();
        ApiFuture<WriteResult> future = bookings.document(bookingId).set(bookingData);
    }

    private void addTicketsToDb(Booking newBooking) {
        for (Ticket t : newBooking.getTickets()) {
            Map<String, Object> ticketData = new HashMap<>();

            ticketData.put("airline", t.getAirline());
            ticketData.put("flightId", t.getFlightId());
            ticketData.put("seatId", t.getSeatId());
            ticketData.put("ticketId", t.getTicketId());
            ticketData.put("customer", t.getCustomer());
            ticketData.put("bookingReference", t.getBookingReference());

            ApiFuture<WriteResult> future = db.collection("bookings").document(newBooking.getId().toString())
                    .collection("tickets").document(t.getTicketId().toString()).set(ticketData);
        }
    }

    private List<Ticket> changeQuotesToTickets(List<Quote> quotesToConfirm, String customer, String bookingReference) {
        List<Ticket> ticketsFromQuotes = new ArrayList<>();
        System.out.println("got to changeQuotesToTickets");
        String bookingRef = UUID.randomUUID().toString();
        for (Quote q : quotesToConfirm) {
//            System.out.println("currentQuote airline: " + q.getAirline());
//            System.out.println("flightId: " + q.getFlightId());
//            System.out.println("currentQuote seatId: " + q.getSeatId());
//            System.out.println("customer=" + customer);
//            System.out.println("bookingReference=" + bookingReference);
//            System.out.println("key=" + API_KEY);
            while (true) {
                try {
                    Ticket ticket = this.webClientBuilder
                            .baseUrl("https://" + q.getAirline())
                            .build()
                            .put()
                            .uri(uriBuilder -> uriBuilder
                                    .pathSegment("flights", q.getFlightId().toString(),
                                            "seats", q.getSeatId().toString(),
                                            "ticket")
                                    .queryParam("customer", customer)
                                    .queryParam("bookingReference", bookingRef)
                                    .queryParam("key", API_KEY)
                                    .build())
                            .retrieve()
                            .onStatus(HttpStatus.CONFLICT::equals,
                                    response -> Mono.error(new seatAlreadyBookedException(q)))
                            .bodyToMono(Ticket.class)
                            .block();
                    ticketsFromQuotes.add(ticket);
                    break;
                } catch (Exception e) {
                    if (e.getClass().equals(seatAlreadyBookedException.class)) {
                        System.out.println("testing...");
                        deleteBookedTicket(ticketsFromQuotes, q);
                    }
                    System.out.println("Exception:" + e.getClass().equals(seatAlreadyBookedException.class));
                    System.out.println("createTicket failed");
                }
            }

        }
        return ticketsFromQuotes;

    }

    private void deleteBookedTicket(List<Ticket> bookedTickets, Quote q) {
        System.out.println("size of booked tickets: " + bookedTickets.size());
        while (true) {
            try {
                Ticket foundTicket = this.webClientBuilder
                        .baseUrl("https://" + q.getAirline())
                        .build()
                        .get()
                        .uri(uriBuilder -> uriBuilder
                                .pathSegment("flights", q.getFlightId().toString(),
                                        "seats", q.getSeatId().toString(),
                                        "ticket")
                                .queryParam("key", API_KEY)
                                .build())
                        .retrieve()
                        .bodyToMono(Ticket.class)
                        .block();
                System.out.println("test: " + foundTicket.toString());
                this.webClientBuilder
                        .baseUrl("https://" + foundTicket.getAirline())
                        .build()
                        .delete()
                        .uri(uriBuilder -> uriBuilder
                                .pathSegment("flights", foundTicket.getFlightId().toString(),
                                        "seats", foundTicket.getSeatId().toString(),
                                        "ticket", foundTicket.getTicketId().toString())
                                .queryParam("key", API_KEY)
                                .build())
                        .retrieve()
                        .bodyToMono(void.class);
                break;
            } catch (Exception e) {
                System.out.println("Ticket not found...");
            }
        }
//        for (Ticket ticket : bookedTickets) {
//            while (true) {
//                try {
//                    this.webClientBuilder
//                            .baseUrl("https://" + ticket.getAirline())
//                            .build()
//                            .delete()
//                            .uri(uriBuilder -> uriBuilder
//                                    .pathSegment("flights", ticket.getFlightId().toString(),
//                                            "seats", ticket.getSeatId().toString(),
//                                            "ticket", ticket.getTicketId().toString())
//                                    .queryParam("key", API_KEY)
//                                    .build())
//                            .retrieve()
//                            .bodyToMono(void.class)
//                            .block();
//                } catch (Exception e) {
//                    System.out.println("trying to delete this ticket");
//                }
//            }
//
//        }
        System.out.println("successfully deleted all booked tickets.");
    }

//    private Ticket createTicket(Quote currentQuote, String bookingRef, String customer) {
//        while (true) {
//            try {
//                return this.webClientBuilder
//                        .baseUrl("https://" + currentQuote.getAirline())
//                        .build()
//                        .put()
//                        .uri(uriBuilder -> uriBuilder
//                                .pathSegment("flights", currentQuote.getFlightId().toString(),
//                                        "seats", currentQuote.getSeatId().toString(),
//                                        "ticket")
//                                .queryParam("customer", customer)
//                                .queryParam("bookingReference", bookingRef)
//                                .queryParam("key", API_KEY)
//                                .build())
//                        .retrieve()
//                        .onStatus(HttpStatus.CONFLICT::equals,
//                                response -> Mono.error(new seatAlreadyBookedException(currentQuote)))
//                        .bodyToMono(Ticket.class)
//                        .block();
//            } catch (Exception e) {
//                if (e.getClass().equals(seatAlreadyBookedException.class)) {
//                    deleteBookedTicket();
//                }
//                System.out.println("Exception:" + e.getClass().equals(seatAlreadyBookedException.class));
//                System.out.println("createTicket failed");
//            }
//        }
//
//
//    }


}
