package com.hazelcast.prometheusdemo;

import java.util.List;
import java.util.Random;

public class PrimeFactorClient
        implements Runnable {

    private final PrimeFactorProvider service;

    public PrimeFactorClient(PrimeFactorProvider service) {
        this.service = service;
    }

    @Override
    public void run() {
        Random random = new Random();
        while (true) {
            int number = Math.abs(random.nextInt());
            List<Integer> primeFactors = service.getPrimeFactors(number);
            System.out.println("prime factors of " + number + ": " + primeFactors);
        }
    }
}
