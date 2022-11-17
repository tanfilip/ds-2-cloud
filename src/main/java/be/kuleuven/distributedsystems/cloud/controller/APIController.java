package be.kuleuven.distributedsystems.cloud.controller;
/*
firebase emulators:start --project demo-distributed-systems-kul
 */

import be.kuleuven.distributedsystems.cloud.auth.WebSecurityConfig;
import be.kuleuven.distributedsystems.cloud.entities.*;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.CollectionModel;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.parameters.P;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import javax.annotation.Resource;
import java.awt.print.Book;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
public class APIController {
    private static  final String DEFAULT_AIRLINE = "reliable-airline.com";
    private static final String API_KEY = "Iw8zeveVyaPNWonPNaU0213uw3g6Ei";

    private List<Booking> allBookings = new ArrayList<>();

    @Resource(name="webClientBuilder")
    private WebClient.Builder webClientBuilder;

    @GetMapping("/getFlights")
    public Collection<Flight> getFlights() {
        return Objects.requireNonNull(this.webClientBuilder
                        .baseUrl("https://" + DEFAULT_AIRLINE)
                        .build()
                        .get()
                        .uri(uriBuilder -> uriBuilder
                                .pathSegment("flights")
                                .queryParam("key", API_KEY)
                                .build())
                        .retrieve()
                        .bodyToMono(new ParameterizedTypeReference<CollectionModel<Flight>>() {
                        })
                        .block())
                .getContent();
    }
    @GetMapping(value = "/getFlight")
    public ResponseEntity<Flight> getFlightById(@RequestParam("airline") String airline,
                                                @RequestParam("flightId") String flightId) {
        Flight requestedFlight = this.webClientBuilder
                .baseUrl("https://" + airline)
                .build()
                .get()
                .uri(uriBuilder -> uriBuilder
                        .pathSegment("flights", flightId)
                        .queryParam("key", API_KEY)
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Flight>() {})
                .block();
        if (requestedFlight != null) {
            return new ResponseEntity<>(requestedFlight, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
        }
    }
//    public Flight getFlight(@RequestParam(name = "airline") String airline,
//                            @RequestParam(name = "flightId") String flightId) {
//        WebClient.ResponseSpec response = this.webClientBuilder
//                .baseUrl("https://" + airline)
//                .build()
//                .get()
//                .uri(uriBuilder -> uriBuilder
//                        .pathSegment("flights", flightId)
//                        .queryParam("key", API_KEY)
//                        .build())
//                .retrieve();
//        Flight desiredFlight = response
//                .bodyToMono(new ParameterizedTypeReference<Flight>() {
//                })
//                .block();
//        if (desiredFlight != null) {
//            return desiredFlight;
//        } else {
//            throw new RuntimeException();
//        }
//    }

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
                        .block())
                .getContent();
        return allFlightTimes.stream().sorted().toList();
    }
/*
"seats" : {
      "href" : "/flights/986195e5-2b53-42c1-aab4-e621cbc0e522/seats?time={time}&available=true",
      "templated" : true
    }
 */
    @GetMapping(value =  "/getAvailableSeats")
    public Map<String, Collection<Seat>> getAvailableSeats(@RequestParam("airline") String airline,
                                                           @RequestParam("flightId") String flightId,
                                                           @RequestParam("time") String time) {
        Collection<Seat> allAvailableSeats = this.webClientBuilder
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
                .bodyToMono(new ParameterizedTypeReference<CollectionModel<Seat>>() {})
                .block()
                .getContent();
        if (allAvailableSeats.size() != 0) {
            Map<String, Collection<Seat>> seatsMappedByType = new HashMap<String, Collection<Seat>>();
            for (Seat currentSeat : allAvailableSeats) {
                if (!seatsMappedByType.containsKey(currentSeat.getType())) {
                    seatsMappedByType.put(currentSeat.getType(), new ArrayList<Seat>());
                }
                seatsMappedByType.get(currentSeat.getType()).add(currentSeat);
            }
            for (String currentType : seatsMappedByType.keySet()) {
                Collection<Seat> sortedType = seatsMappedByType.get(currentType)
                        .stream().sorted((s1, s2) -> s1.getName().compareTo(s2.getName())).toList();
                seatsMappedByType.put(currentType, sortedType);
            }
            return seatsMappedByType;
        } else {
            throw new RuntimeException();
        }

    }

//    https://reliable-airline.com
//    /flights/986195e5-2b53-42c1-aab4-e621cbc0e522/seats
//    ?time=2022-12-06T18:10:00&available=true&key=Iw8zeveVyaPNWonPNaU0213uw3g6Ei
    @GetMapping(value = "/getSeat")
    public Seat getSeat(@RequestParam("airline") String airline,
                        @RequestParam("flightId") String flightId,
                        @RequestParam("seatId") String seatId) {
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
    }

    @PostMapping(value = "/confirmQuotes")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    // https://reliable-airlines.com
//    /flights/986195e5-2b53-42c1-aab4-e621cbc0e522/seats/b06d1919-0fa0-417a-a006-3a82e6e51bf5/ticket
//    ?customer=&bookingReference=&key=Iw8zeveVyaPNWonPNaU0213uw3g6Ei
    public void confirmQuotes(@RequestBody List<Quote> quotes) {

        List<Ticket> quoteTickets = new ArrayList<>();
        UUID bookingId = UUID.randomUUID();
        User user = WebSecurityConfig.getUser();
        for (Quote currentQuote : quotes) {
            quoteTickets.add(this.webClientBuilder
                    .baseUrl("https://" + currentQuote.getAirline())
                    .build()
                    .put()
                    .uri(uriBuilder -> uriBuilder
                            .pathSegment("flights", currentQuote.getFlightId().toString(),
                                    "seats", currentQuote.getSeatId().toString(),
                                    "ticket")
                            .queryParam("customer",user)
                            .queryParam("bookingReference", "")
                            .queryParam("key", API_KEY)
                            .build())
                    .retrieve()
                    .bodyToMono(new ParameterizedTypeReference<Ticket>() {
                    }).block());
        }

        Booking newBooking = new Booking(bookingId, LocalDateTime.now(), quoteTickets, user.getEmail());
        allBookings.add(newBooking);
    }

    @GetMapping(value = "/getBookings")
    public List<Booking> getBookings() {
        List<Booking> customerBookings = new ArrayList<>();
        User currentUser = WebSecurityConfig.getUser();
        for (Booking b : getAllBookingsFromDb()) {
            if (b.getCustomer().equalsIgnoreCase(currentUser.getEmail())) {
                customerBookings.add(b);
            }
        }
        return customerBookings;
    }

    @GetMapping(value = "/getAllBookings")
    public List<Booking> getAllBookings() {
        User user = WebSecurityConfig.getUser();
        if (user.isManager()) {
            return getAllBookingsFromDb();
        }
        return null;
    }

    @GetMapping(value =  "/getBestCustomers")
    public List<String> getBestCustomers() {
        User user = WebSecurityConfig.getUser();
        if (!user.isManager()) {
            return null;
        }
        Map<String, Integer> customerBookings = mapBookingsPerCustomer();

        return new ArrayList<>(customerBookings.keySet());

    }

    private Map<String, Integer> mapBookingsPerCustomer() {
        Map<String, Integer> customers = new HashMap<>();

        for (Booking b : getAllBookingsFromDb()) {
            if (!customers.containsKey(b.getCustomer())) {
                customers.put(b.getCustomer(), 0);
            }
            customers.put(b.getCustomer(), customers.get(b.getCustomer()) + b.getTickets().size());
        }
        return customers;
    }

    public List<Ticket> getTicketsFromQuotes(List<Quote> allQuotes) {
        List<Ticket> allTickets = new ArrayList<>();
        for (Quote q : allQuotes) {
            Ticket newTicket = this.webClientBuilder
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
                    .bodyToMono(new ParameterizedTypeReference<Ticket>() {})
                    .block();
            allTickets.add(newTicket);
        }
        return allTickets;
    }

    public List<Booking> getAllBookingsFromDb() {
        return allBookings;
    }
}
