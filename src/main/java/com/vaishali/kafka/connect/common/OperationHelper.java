package com.vaishali.kafka.connect.common;


import static java.lang.Thread.sleep;


/**
 * Created by ang.chen on 1/4/17.
 */
public class OperationHelper {
    public static void doWithRetry(int maxAttempts, int sleepTime, Operation<Boolean> operation) {
        int count = 0;
        for (; count < maxAttempts; count++) {
            try {
                if (operation.doIt()) {
                    break; // Don't retry if succeeded
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.err.println("Failed for " + (count + 1) + " time");
            }
            try {
                sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (count >= maxAttempts) {
            throw new RetryException(count);
        }
    }

    public static <T extends Object> T doWithReturnRetry(int maxAttempts, int sleepTime, Operation<T> operation) {
        for (int count = 0; count < maxAttempts; count++) {
            try {
                return operation.doIt();
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed for " + (count + 1) + " time");
            }
            try {
                sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}