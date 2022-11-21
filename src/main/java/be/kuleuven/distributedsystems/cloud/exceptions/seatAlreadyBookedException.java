package be.kuleuven.distributedsystems.cloud.exceptions;

import be.kuleuven.distributedsystems.cloud.entities.Quote;

public class seatAlreadyBookedException extends RuntimeException {
    public seatAlreadyBookedException(Quote currentQuote) {
        super("could not book a ticket on seat " + currentQuote.getSeatId()
        + " on flight " + currentQuote.getFlightId() + " on airline "
        + currentQuote.getAirline());
    }
}
