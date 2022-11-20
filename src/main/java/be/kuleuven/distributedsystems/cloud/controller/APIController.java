package be.kuleuven.distributedsystems.cloud.controller;
/*
firebase emulators:start --project demo-distributed-systems-kul
 */

import be.kuleuven.distributedsystems.cloud.auth.WebSecurityConfig;
import be.kuleuven.distributedsystems.cloud.entities.*;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.CollectionModel;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;

@RestController
@RequestMapping("/api")
public class APIController {
    private static  final List<String> AIRLINES = new ArrayList<>();

//    private static final String UNRELIABLE_AIRLINE = "unreliable-airline.com";
    private static final String API_KEY = "Iw8zeveVyaPNWonPNaU0213uw3g6Ei";

    private List<Booking> allBookings = new ArrayList<>();

    @Resource(name="webClientBuilder")
    private WebClient.Builder webClientBuilder;


    public APIController() {
        AIRLINES.add("reliable-airline.com");
        AIRLINES.add("unreliable-airline.com");
    }
    @GetMapping("/getFlights")
    public Collection<Flight> getFlights() {
        List<Flight> allFlights = new ArrayList<>();
//        boolean flightsNotFound;
        for (String currentAirline : AIRLINES) {
//            boolean flightsNotFound = true;
            while (true) {
                try {
                    Collection<Flight> currentFlights = Objects.requireNonNull(this.webClientBuilder
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
                                    .block())
                            .getContent();
                    allFlights.addAll(currentFlights);
                    break;
//                    flightsNotFound = false;
                } catch (Exception e) {
                    System.out.println("getFlights1: " + currentAirline + " failed to find flights, try again...");
                }
            }


        }
        return allFlights;
    }
    @GetMapping(value = "/getFlight")
    public Flight getFlightById(@RequestParam("airline") String airline,
                                                @RequestParam("flightId") String flightId) {
//        boolean flightNotFound = true;
//        Flight requestedFlight = null;

        while (true) {
            try {
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
                        .block();
            } catch (Exception e) {
                System.out.println("\r\nGetFlight2: " + airline + " failed to find flight"+ flightId +"\r\n");
            }
        }
    }

    @GetMapping(value = "/getFlightTimes")
    public Collection<LocalDateTime> getFlightTimes(@RequestParam("airline") String airline,
                                                    @RequestParam("flightId") String flightId) {
        while (true) {
            try {
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
            } catch (Exception e) {
                System.out.println("getFlightTimes3: " + airline + " couldn't find time");
            }
        }

    }

    @GetMapping(value =  "/getAvailableSeats")
    public Map<String, Collection<Seat>> getAvailableSeats(@RequestParam("airline") String airline,
                                                           @RequestParam("flightId") String flightId,
                                                           @RequestParam("time") String time) {
        while (true) {
            try {
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
                Map<String, Collection<Seat>> seatsMappedByType = mapSeatsByType(allAvailableSeats);

                for (String currentType : seatsMappedByType.keySet()) {
                    Collection<Seat> sortedType = seatsMappedByType.get(currentType)
                            .stream().sorted((seat1, seat2) -> compareSeats(seat1.getName(), seat2.getName())).toList();
                    seatsMappedByType.put(currentType, sortedType);
                }

                return seatsMappedByType;
            } catch (Exception e) {
                System.out.println("getAvailableSeats4: " + airline + "Error");
            }
        }
    }

    private Map<String, Collection<Seat>> mapSeatsByType(Collection<Seat> allAvailableSeats) {
        Map<String, Collection<Seat>> seatsMappedByType = new HashMap<String, Collection<Seat>>();
        for (Seat currentSeat : allAvailableSeats) {
            if (!seatsMappedByType.containsKey(currentSeat.getType())) {
                seatsMappedByType.put(currentSeat.getType(), new ArrayList<Seat>());
            }
            seatsMappedByType.get(currentSeat.getType()).add(currentSeat);
        }
        return seatsMappedByType;
    }

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

    public void confirmQuotes(@RequestBody List<Quote> quotes) {
        List<Ticket> tickets = new ArrayList<>();
        User user = WebSecurityConfig.getUser();
        String customer = user.getEmail();
        for (Quote currentQuote : quotes) {
            while (true) {
                try {
                    Ticket ticket = createTicket(currentQuote, customer);
                    tickets.add(ticket);
                    break;
                } catch (Exception e) {
                    System.out.println("confirmQuotes6: Pause");
                }
            }
        }
        UUID bookingId = UUID.randomUUID();
        Booking newBooking = new Booking(bookingId, LocalDateTime.now(), tickets, customer);
        addBooking(newBooking);
    }

    private void addBooking(Booking newBooking) {
        allBookings.add(newBooking);
    }

    private Ticket createTicket(Quote currentQuote, String customer) {
        return this.webClientBuilder
                .baseUrl("https://" + currentQuote.getAirline())
                .build()
                .put()
                .uri(uriBuilder -> uriBuilder
                        .pathSegment("flights", currentQuote.getFlightId().toString(),
                                "seats", currentQuote.getSeatId().toString(),
                                "ticket")
                        .queryParam("customer", customer)
                        .queryParam("bookingReference", "")
                        .queryParam("key", API_KEY)
                        .build())
                .retrieve()
                .bodyToMono(Ticket.class)
                .block();
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
        List<String> bestCustomers = new ArrayList<>();
        Map<String, Integer> customerBookings = mapBookingsPerCustomer();
        Integer maxvalue = Collections.max(customerBookings.values());

        for (String c : customerBookings.keySet()) {
            Integer value = customerBookings.get(c);
            if (Objects.equals(value, maxvalue))
                bestCustomers.add(c);
        }

        return bestCustomers;

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

    public List<Booking> getAllBookingsFromDb() {
        return allBookings;
    }


}