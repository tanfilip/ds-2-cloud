package be.kuleuven.distributedsystems.cloud.controller;
/*
firebase emulators:start --project demo-distributed-systems-kul
 */

import be.kuleuven.distributedsystems.cloud.auth.WebSecurityConfig;
import be.kuleuven.distributedsystems.cloud.entities.*;
import com.google.api.core.ApiFuture;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.firestore.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.pubsub.v1.PubsubMessage;
import org.eclipse.jetty.util.ajax.JSON;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.CollectionModel;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.util.retry.Retry;

import javax.annotation.Resource;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api")
public class APIController {
    private static  final List<String> AIRLINES = new ArrayList<>();

    private static final String API_KEY = "Iw8zeveVyaPNWonPNaU0213uw3g6Ei";


    private final Gson gson = new Gson();

    private final JsonParser jsonParser = new JsonParser();

    @Resource(name = "db")
    Firestore db;
    @Resource(name = "projectId")
    private String projectId;

    @Resource(name = "topicId")
    private String topicId;

    @Resource(name="webClientBuilder")
    private WebClient.Builder webClientBuilder;

    @Resource(name="publisher")
    private Publisher publisher;

    @Resource(name = "pubSubTransportChannelProvider")
    private TransportChannelProvider channelProvider;

    @Resource(name = "pubSubCredentialsProvider")
    private CredentialsProvider credentialsProvider;


    /**
     * Initialize all airlines.
     */
    public APIController() {
        AIRLINES.add("reliable-airline.com");
        AIRLINES.add("unreliable-airline.com");
    }

    /**
     *
     * @return all the flights hosted by reliable-airline.com and unreliable-airline.com
     */
    @GetMapping("/getFlights")
    public Collection<Flight> getFlights() {
        List<Flight> allFlights = new ArrayList<>();
//        boolean flightsNotFound;
        for (String currentAirline : AIRLINES) {
//            boolean flightsNotFound = true;
            Collection<Flight> currentFlights = this.webClientBuilder
                    .baseUrl("https://" + currentAirline)
                    .build()
                    .get()
                    .uri(uriBuilder -> uriBuilder
                            .pathSegment("flights")
                            .queryParam("key", API_KEY)
                            .build())
                    .retrieve()
                    .bodyToMono(new ParameterizedTypeReference<CollectionModel<Flight>>() {
                    })
                    .retryWhen(Retry.max(3))
                    .block()
                    .getContent();
            allFlights.addAll(currentFlights);



        }
        return allFlights;
    }
    @GetMapping(value = "/getFlight")
    public Flight getFlightById(@RequestParam("airline") String airline,
                                                @RequestParam("flightId") String flightId) {
        return this.webClientBuilder
                .baseUrl("https://" + airline)
                .build()
                .get()
                .uri(uriBuilder -> uriBuilder
                        .pathSegment("flights", flightId)
                        .queryParam("key", API_KEY)
                        .build())
                .retrieve()
                .bodyToMono(Flight.class)
                .retryWhen(Retry.max(3))
                .block();
    }

    @GetMapping(value = "/getFlightTimes")
    public Collection<LocalDateTime> getFlightTimes(@RequestParam("airline") String airline,
                                                    @RequestParam("flightId") String flightId) {
        Collection<LocalDateTime> allFlightTimes = Objects.requireNonNull(this.webClientBuilder
                        .baseUrl("https://" + airline)
                        .build()
                        .get()
                        .uri(uriBuilder -> uriBuilder
                                .pathSegment("flights", flightId, "times")
                                .queryParam("key", API_KEY)
                                .build())
                        .retrieve()
                        .bodyToMono(new ParameterizedTypeReference<CollectionModel<LocalDateTime>>() {
                        })
                        .retryWhen(Retry.max(3))
                        .block())
                .getContent();
        return allFlightTimes.stream().sorted().toList();

    }

    @GetMapping(value =  "/getAvailableSeats")
    public Map<String, Collection<Seat>> getAvailableSeats(@RequestParam("airline") String airline,
                                                           @RequestParam("flightId") String flightId,
                                                           @RequestParam("time") String time) {
        Collection<Seat> allAvailableSeats = Objects.requireNonNull(this.webClientBuilder
                        .baseUrl("https://" + airline)
                        .build()
                        .get()
                        .uri(uriBuilder -> uriBuilder
                                .pathSegment("flights", flightId, "seats")
                                .queryParam("time", time)
                                .queryParam("available", "true")
                                .queryParam("key", API_KEY)
                                .build())
                        .retrieve()
                        .bodyToMono(new ParameterizedTypeReference<CollectionModel<Seat>>() {
                        })
                        .retryWhen(Retry.max(3))
                        .block())
                .getContent();
        Map<String, Collection<Seat>> seatsMappedByType = mapSeatsByType(allAvailableSeats);

        for (String currentType : seatsMappedByType.keySet()) {
            Collection<Seat> sortedType = seatsMappedByType.get(currentType)
                    .stream().sorted((seat1, seat2) -> compareSeats(seat1.getName(), seat2.getName())).toList();
            seatsMappedByType.put(currentType, sortedType);
        }

        return seatsMappedByType;

    }

    private Map<String, Collection<Seat>> mapSeatsByType(Collection<Seat> allAvailableSeats) {
        Map<String, Collection<Seat>> seatsMappedByType = new HashMap<>();
        for (Seat currentSeat : allAvailableSeats) {
            if (!seatsMappedByType.containsKey(currentSeat.getType())) {
                seatsMappedByType.put(currentSeat.getType(), new ArrayList<>());
            }
            seatsMappedByType.get(currentSeat.getType()).add(currentSeat);
        }
        return seatsMappedByType;
    }

    /**
     * Compares 2 seat names according to their location on the plane.
     * @param seat1 the seat that's compared to seat2.
     * @param seat2 the seat that's compared by.
     * @return  a positive number if seat1 takes place after seat2.
     *          a negative number if seat1 takes place before seat2.
     *          0 if seat1 and seat2 takes place on the same place.
     */
    private int compareSeats(String seat1, String seat2) {
        if (seat1.length() > seat2.length()) {
            return 1;
        } else if (seat1.length() < seat2.length()) {
            return -1;
        } else {
            return seat1.compareTo(seat2);
        }
    }

    @GetMapping(value = "/getSeat")
    public Seat getSeat(@RequestParam("airline") String airline,
                        @RequestParam("flightId") String flightId,
                        @RequestParam("seatId") String seatId) {
        while (true) {
            try {
                return this.webClientBuilder
                        .baseUrl("https://" + airline)
                        .build()
                        .get()
                        .uri(uriBuilder -> uriBuilder
                                .pathSegment("flights", flightId, "seats", seatId)
                                .queryParam("key", API_KEY)
                                .build())
                        .retrieve()
                        .bodyToMono(new ParameterizedTypeReference<Seat>() {})
                        .block();
            } catch (Exception e) {
                System.out.println("getSeat5: Pause");
            }
        }
    }

    @PostMapping(value = "/confirmQuotes")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void confirmQuotes(@RequestBody List<Quote> quotesToConfirm) throws ExecutionException, InterruptedException {

        // Publishing a message:

        // Get customer's name and email.
        User user = WebSecurityConfig.getUser();
        String customer = user.getEmail();

        String quotesAsJSON = gson.toJson(quotesToConfirm);
        PubsubMessage pubSubMsg = PubsubMessage.newBuilder()
                .putAttributes("quotes", quotesAsJSON)
                .putAttributes("customer", customer)
                .build();

        // Once published, returns a server-assigned message id
        if (publisher != null) {
            ApiFuture<String> future = publisher.publish(pubSubMsg);
            future.get();
        }
    }

    @GetMapping(value = "/getBookings")
    public List<Booking> getBookings() throws ExecutionException, InterruptedException {
        List<Booking> customerBookings = new ArrayList<>();
        User currentUser = WebSecurityConfig.getUser();
        List<Booking> allBookings = getAllBookingsFromDb();
        for (Booking b : allBookings) {
            if (b.getCustomer().equalsIgnoreCase(currentUser.getEmail())) {
                customerBookings.add(b);
            }
        }
        return customerBookings;
    }

    @GetMapping(value = "/getAllBookings")
    public List<Booking> getAllBookings() throws ExecutionException, InterruptedException {
        User user = WebSecurityConfig.getUser();
        if (user.isManager()) {
            return getAllBookingsFromDb();
        }
        return new ArrayList<>();
    }

    @GetMapping(value =  "/getBestCustomers")
    public List<String> getBestCustomers() throws ExecutionException, InterruptedException {
        User user = WebSecurityConfig.getUser();
        if (!user.isManager()) {
            return null;
        }
        List<String> bestCustomers = new ArrayList<>();
        Map<String, Integer> customerBookings = mapCustomersByTicketAmount();
        Integer maxvalue = Collections.max(customerBookings.values());

        for (String c : customerBookings.keySet()) {
            Integer value = customerBookings.get(c);
            if (Objects.equals(value, maxvalue))
                bestCustomers.add(c);
        }
        return bestCustomers;
    }

    /**
     * Categorize the customers according to their total amount of tickets.
     * @return Map<customerA, amount of tickets of customerA>
     */
    private Map<String, Integer> mapCustomersByTicketAmount() throws ExecutionException, InterruptedException {
        Map<String, Integer> customers = new HashMap<>();

        for (Booking b : getAllBookingsFromDb()) {
            if (!customers.containsKey(b.getCustomer())) {
                customers.put(b.getCustomer(), 0);
            }
            customers.put(b.getCustomer(), customers.get(b.getCustomer()) + b.getTickets().size());
        }
        return customers;
    }

    private List<Booking> getAllBookingsFromDb() throws ExecutionException, InterruptedException {
        List<Booking> allRetrievedBookings = new ArrayList<>();
        System.out.println("begin of getAllBookingsFromDb");
        //database stores bookings as: bookingId

        // All bookings:
        List<QueryDocumentSnapshot> allDocuments = db.collection("bookings").get().get().getDocuments();

        // Queries:
        System.out.println("allDocuments size: " + allDocuments.size());
//        for (QueryDocumentSnapshot currentBooking : allDocuments) {
//            String bookingId = currentBooking.getData().get("id").toString();
//            List<Ticket> bookingTickets = getTicketsFromBookingInDb(bookingId);
//
//            LocalDateTime bookingTime = LocalDateTime.parse(currentBooking.getData().get("time").toString());
//            String bookingCustomer = currentBooking.getData().get("customer").toString();
//            Booking retrievedBooking = new Booking(UUID.fromString(bookingId), bookingTime, bookingTickets, bookingCustomer);
//            allRetrievedBookings.add(retrievedBooking);
//        }
        for (QueryDocumentSnapshot currentBooking : allDocuments) {
            String bookingId = currentBooking.getData().get("id").toString();
            String ticketString = currentBooking.getData().get("tickets").toString();
            JsonElement ticketsAsJSON = jsonParser.parse(ticketString);
            Type listTicketType = new TypeToken<ArrayList<Ticket>>() {}.getType();
            List<Ticket> bookingTickets = gson.fromJson(ticketsAsJSON, listTicketType);



            LocalDateTime bookingTime = LocalDateTime.parse(currentBooking.getData().get("time").toString());
            String bookingCustomer = currentBooking.getData().get("customer").toString();
            Booking retrievedBooking = new Booking(UUID.fromString(bookingId), bookingTime, bookingTickets, bookingCustomer);
            allRetrievedBookings.add(retrievedBooking);
        }
        return allRetrievedBookings;
    }

//    private List<Ticket> getTicketsFromBookingInDb(String bookingId) throws ExecutionException, InterruptedException {
//        List<Ticket> bookingTickets = new ArrayList<>();
//
//        CollectionReference bookings = db.collection("bookings").document(bookingId).collection("tickets");
//        List<QueryDocumentSnapshot> ticketQueries = bookings.get().get().getDocuments();
//        for (QueryDocumentSnapshot ticket : ticketQueries) {
//            String ticketId = ticket.getData().get("ticketId").toString();
//            DocumentSnapshot t = bookings.document(ticketId).get().get();
//            String airline = t.getData().get("airline").toString();
//            UUID flightId = UUID.fromString(t.getData().get("flightId").toString());
//            UUID seatId = UUID.fromString(t.getData().get("seatId").toString());
//            UUID ticketIdAsUUID = UUID.fromString(ticketId);
//            String customer = t.getData().get("customer").toString();
//            String bookingReference = t.getData().get("bookingReference").toString();
//
//            Ticket retrievedTicket = new Ticket(airline, flightId,seatId, ticketIdAsUUID, customer, bookingReference);
//            bookingTickets.add(retrievedTicket);
//        }
//
//        return bookingTickets;
//    }


}