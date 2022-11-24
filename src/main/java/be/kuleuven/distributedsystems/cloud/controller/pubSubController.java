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
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.annotation.Resource;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

@RestController
public class pubSubController {
    private static final String API_KEY = "Iw8zeveVyaPNWonPNaU0213uw3g6Ei";

    private final Gson gson = new Gson();
    private final JsonParser jsonParser = new JsonParser();
    @Resource(name="webClientBuilder")
    private WebClient.Builder webClientBuilder;
    @Resource(name = "db")
    Firestore db;

    @PostMapping("/subs")
    public ResponseEntity<String> subscription(@RequestBody String body) throws ExecutionException, InterruptedException {
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

        List<Ticket> ticketsFromQuotes = getTickets(quotesToConfirm, customer, bookingReference);
        System.out.println("getTicketsfinished");
        if (ticketsFromQuotes.size() != 0) {
            addBookingToDatabase(ticketsFromQuotes, customer, bookingReference);
        }
        return ResponseEntity.status(HttpStatus.OK).body("OK");

    }

    private List<Ticket> getTickets(List<Quote> quotesToConfirm, String customer, String bookingReference) {
        List<Ticket> ticketsFromQuotes = new ArrayList<>();
        System.out.println("got to getTickets");
        System.out.println("size quotes: " + quotesToConfirm.size());
        try {
            for (Quote q: quotesToConfirm) {
                Ticket ticket = putTicket(q, customer, bookingReference);
                ticketsFromQuotes.add(ticket);
                System.out.println("ticket added");
            }
        } catch (Exception e) {
            System.out.println("putTicket failed");
            deleteBookedTicket(ticketsFromQuotes);
        }

        return ticketsFromQuotes;
    }

    private Ticket putTicket(Quote q, String customer, String bookingReference) {
        System.out.println("got to putTicket");
        return this.webClientBuilder
                .baseUrl("https://" + q.getAirline())
                .build()
                .put()
                .uri(uriBuilder -> uriBuilder
                        .pathSegment("flights", q.getFlightId().toString(),
                                "seats", q.getSeatId().toString(),
                                "ticket")
                        .queryParam("customer", customer)
                        .queryParam("bookingReference", bookingReference)
                        .queryParam("key", API_KEY)
                        .build())
                .retrieve()
                .onStatus(HttpStatus.CONFLICT::equals,
                        response -> Mono.error(new seatAlreadyBookedException(q)))
                .bodyToMono(new ParameterizedTypeReference<Ticket>() {
                })
                .retry(3)
//                .retryWhen(Retry.max(3))
                .block();
    }

    private Ticket getBookedTicket(Quote q, String customer) {
        // Get-ticket:
        System.out.println("Got to getBookedTicket");
        System.out.println("https://" + q.getAirline()  + "/flights/" + q.getFlightId() + "/seats/" + q.getSeatId() + "/ticket?key=" + API_KEY);
        return this.webClientBuilder
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
                .bodyToMono(new ParameterizedTypeReference<Ticket>() {
                })
//                .retryWhen(Retry.max(3))
                .block();
    }

    private void addBookingToDatabase(List<Ticket> ticketsFromQuotes, String customer, String bookingReference) throws ExecutionException, InterruptedException {
        String currentBookingTime = LocalDateTime.now().toString();
        System.out.println("addBookingToDatabase");
        System.out.println(ticketsFromQuotes.size());
        String ticketsAsJSON = gson.toJson(ticketsFromQuotes);

        // BookingData is structured like booking => contains id, time, tickets and customers.
        Map<String, Object> bookingData = new HashMap<>();
        bookingData.put("id", bookingReference);
        bookingData.put("time", currentBookingTime);
        bookingData.put("tickets", ticketsAsJSON);
        bookingData.put("customer", customer);

        ApiFuture<WriteResult> future = db.collection("bookings").document(bookingReference).set(bookingData);
        future.get();
    }

//    private void addTicketsToDb(Booking newBooking) throws ExecutionException, InterruptedException {
//        for (Ticket t : newBooking.getTickets()) {
//            Map<String, Object> ticketData = new HashMap<>();
//
//            ticketData.put("airline", t.getAirline());
//            ticketData.put("flightId", t.getFlightId().toString());
//            ticketData.put("seatId", t.getSeatId().toString());
//            ticketData.put("ticketId", t.getTicketId().toString());
//            ticketData.put("customer", t.getCustomer());
//            ticketData.put("bookingReference", t.getBookingReference());
//
//            ApiFuture<WriteResult> future = db.collection("bookings").document(newBooking.getId().toString())
//                    .collection("tickets").document(t.getTicketId().toString()).set(ticketData);
//            future.get();
//            System.out.println("Ticket successfully added.");
//        }
//    }


    private void deleteBookedTicket(List<Ticket> bookedTickets) {
        // delete-ticket;
        System.out.println("size of booked tickets: " + bookedTickets.size());
        for (Ticket t: bookedTickets) {
            this.webClientBuilder
                    .baseUrl("https://" + t.getAirline())
                    .build()
                    .delete()
                    .uri(uriBuilder -> uriBuilder
                            .pathSegment("flights", t.getFlightId().toString(),
                                    "seats", t.getSeatId().toString(),
                                    "ticket", t.getTicketId().toString())
                            .queryParam("key", API_KEY)
                            .build())
                    .retrieve()
                    .bodyToMono(void.class)
                    .retry(3)
                    .block();
            System.out.println("succesfully deleted 1");
        }
        System.out.println("successfully deleted all booked tickets.");
    }
}
