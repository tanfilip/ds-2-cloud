package be.kuleuven.distributedsystems.cloud.controller;

import be.kuleuven.distributedsystems.cloud.entities.Quote;
import be.kuleuven.distributedsystems.cloud.entities.Ticket;
import be.kuleuven.distributedsystems.cloud.exceptions.seatAlreadyBookedException;
import com.google.api.core.ApiFuture;
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
    public ResponseEntity<String> confirmQuotes(@RequestBody String body) throws ExecutionException, InterruptedException {
        JsonElement jsonRoot = jsonParser.parse(body);
        JsonElement msg = jsonRoot.getAsJsonObject().get("message");
        JsonElement attributes = msg.getAsJsonObject().get("attributes");

        // Receive the pushed pubsub message.
        JsonElement quotesAsJson = attributes.getAsJsonObject().get("quotes");
        JsonElement customerAsJson = attributes.getAsJsonObject().get("customer");

        // Deserialize the quotes and customer.
        Type listQuoteType = new TypeToken<ArrayList<Quote>>() {}.getType();
        List<Quote> quotesToConfirm = gson.fromJson((String) JSON.parse(String.valueOf(quotesAsJson)), listQuoteType);
        String customer = gson.fromJson((String) JSON.parse(String.valueOf(customerAsJson)), String.class);

        String bookingReference = UUID.randomUUID().toString();

        // try to make PUT requests.
        List<Ticket> ticketsFromQuotes = getTickets(quotesToConfirm, customer, bookingReference);
        System.out.println("getTicketsfinished");
        if (ticketsFromQuotes.size() != 0) {
            addBookingToDatabase(ticketsFromQuotes, customer, bookingReference);
        }
        return ResponseEntity.status(HttpStatus.OK).body("OK");

    }

    /**
     * Try to make PUT request for each quote, if one PUT request fails, delete them all.
     * @param quotesToConfirm quotes given by publisher, that need to be turned into tickets.
     * @param customer the customer's email address used as customer reference.
     * @param bookingReference reference to booking ID
     * @return list of tickets gotten from quotes.
     */
    private List<Ticket> getTickets(List<Quote> quotesToConfirm, String customer, String bookingReference) {
        List<Ticket> ticketsFromQuotes = new ArrayList<>();
        try {
            for (Quote q: quotesToConfirm) {
                Ticket ticket = putTicket(q, customer, bookingReference);
                ticketsFromQuotes.add(ticket);
            }
        } catch (Exception e) {
            deleteBookedTicket(ticketsFromQuotes);
        }

        return ticketsFromQuotes;
    }

    /**
     * sends a PUT request to the webserver to get the ticket and makes the seat taken.
     * @param quote received quote
     * @param customer the customer's email address.
     * @param bookingReference reference to booking ID
     * @return the ticket for the taken seat.
     */

    private Ticket putTicket(Quote quote, String customer, String bookingReference) {
        System.out.println("got to putTicket");
        return this.webClientBuilder
                .baseUrl("https://" + quote.getAirline())
                .build()
                .put()
                .uri(uriBuilder -> uriBuilder
                        .pathSegment("flights", quote.getFlightId().toString(),
                                "seats", quote.getSeatId().toString(),
                                "ticket")
                        .queryParam("customer", customer)
                        .queryParam("bookingReference", bookingReference)
                        .queryParam("key", API_KEY)
                        .build())
                .retrieve()
                .onStatus(HttpStatus.CONFLICT::equals,
                        response -> Mono.error(new seatAlreadyBookedException(quote)))
                .bodyToMono(new ParameterizedTypeReference<Ticket>() {
                })
                .retryWhen(Retry.max(3))
//                .retryWhen(Retry.max(3))
                .block();
    }

    /**
     * add the booking to the Firestore database as a JSON.
     * @param ticketsFromQuotes list of tickets that need to be booked.
     * @param customer the customer's email address, used as customer's reference.
     * @param bookingId random UUID as string created to identify a booking
     * @throws ExecutionException when storing the booking fails.
     * @throws InterruptedException when storing the booking is interrupted by something.
     */
    private void addBookingToDatabase(List<Ticket> ticketsFromQuotes, String customer, String bookingId) throws ExecutionException, InterruptedException {
        String currentBookingTime = LocalDateTime.now().toString();
        String ticketsAsJSON = gson.toJson(ticketsFromQuotes);

        // BookingData is structured like booking => contains id, time, tickets and customers.
        Map<String, Object> bookingData = new HashMap<>();
        bookingData.put("id", bookingId);
        bookingData.put("time", currentBookingTime);
        bookingData.put("tickets", ticketsAsJSON);
        bookingData.put("customer", customer);

        ApiFuture<WriteResult> future = db.collection("bookings").document(bookingId).set(bookingData);
        future.get();
    }

    /**
     * if a seat in the cart is taken, delete all the already booked tickets in the cart.
     * @param bookedTickets the already booked tickets.
     */
    private void deleteBookedTicket(List<Ticket> bookedTickets) {
        // delete-ticket;
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
                    .retryWhen(Retry.max(3))
                    .block();
        }
    }
}