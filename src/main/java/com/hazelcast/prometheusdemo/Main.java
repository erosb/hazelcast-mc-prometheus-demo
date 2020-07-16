package com.hazelcast.prometheusdemo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {

    public static final Random RANDOM = new Random();

    static class PrimeFactorizer
            implements Runnable, Serializable, HazelcastInstanceAware {

        private List<Long> primeFactors() {
            List<Long> factors = new ArrayList<>();
            long number = this.number;
            for (long i = 2; i < number; i++) {
                while (number % i == 0) {
                    factors.add(i);
                    number /= i;
                }
            }
            if (number > 2) {
                factors.add(number);
            }
            return factors;
        }

        private final long number;

        private IMap<Long, List<Long>> destination;

        public PrimeFactorizer(long number) {
            this.number = number;
        }

        @Override
        public void run() {
            System.out.println("RECV " + number);
            List<Long> primeFactors = primeFactors();
            System.out.println(number + " -> " + primeFactors);
            destination.put(number, primeFactors);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            destination = hazelcastInstance.getMap("destination");
        }
    }

    public static void main(String[] args) {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        ITopic<Long> topic = instance.getTopic("messages");

        IExecutorService executorService = instance.getExecutorService("executor-service");

        new Thread(() -> {
            for (int i = 0; i < 1_000; ++i) {
                topic.publish((long) Math.abs(RANDOM.nextInt()));
            }
        }).start();

        topic.addMessageListener(message -> executorService.execute(new PrimeFactorizer(message.getMessageObject())));
    }
}
