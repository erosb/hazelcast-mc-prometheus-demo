package com.hazelcast.prometheusdemo;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class PrimeFactorProvider {

    static class PrimeFactorizer
            implements Callable<List<Integer>>, Serializable, HazelcastInstanceAware {

        private List<Integer> primeFactors() {
            List<Integer> factors = new ArrayList<>();
            int number = this.number;
            for (int i = 2; i < number; i++) {
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

        private final int number;

        private IMap<Integer, List<Integer>> destination;

        public PrimeFactorizer(int number) {
            this.number = number;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            destination = hazelcastInstance.getMap("primeFactors");
        }

        @Override
        public List<Integer> call()
                throws Exception {
//            System.out.println("RECV " + number);
            List<Integer> primeFactors = primeFactors();
//            System.out.println(number + " -> " + primeFactors);
            destination.put(number, primeFactors);
            return primeFactors;
        }
    }
    private final ITopic<Integer> toBeCalculated;
    private final IExecutorService executor;

    private final IMap<Integer, List<Integer>> primeFactors;

    public PrimeFactorProvider(ITopic<Integer> toBeCalculated, IExecutorService executor,
                               IMap<Integer, List<Integer>> primeFactors) {
        this.toBeCalculated = toBeCalculated;
        this.executor = executor;
        this.primeFactors = primeFactors;
        toBeCalculated.addMessageListener(message -> loadPrimeFactors(message.getMessageObject()));
    }

    private void loadPrimeFactors(Integer number) {
        executor.submit(new PrimeFactorizer(number));
    }
    
    public List<Integer> getPrimeFactors(Integer number) {
        if (primeFactors.containsKey(number)) {
            return primeFactors.get(number);
        }
        try {
            return executor.submit(new PrimeFactorizer(number)).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public void startPreloading() {
        Random random = new Random();
        new Thread(() -> {
            for (int i = 0; i < 1_000; ++i) {
                toBeCalculated.publish(Math.abs(random.nextInt()));
            }
        }).start();
    }
    
}
