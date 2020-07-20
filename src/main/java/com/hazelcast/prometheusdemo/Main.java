package com.hazelcast.prometheusdemo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.topic.ITopic;

public class Main {

    public static void main(String[] args) {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        
        ITopic<Integer> topic = instance.getTopic("messages");

        IExecutorService executorService = instance.getExecutorService("executor-service");

        PrimeFactorProvider service = new PrimeFactorProvider(topic, executorService, instance.getMap("primeFactors"));
        service.startPreloading();
        
        new Thread(new PrimeFactorClient(service)).start();
    }
    
}
