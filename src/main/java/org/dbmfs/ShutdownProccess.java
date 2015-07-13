package org.dbmfs;

public class ShutdownProccess extends Thread {

    public void run() {
        System.out.println("DbmFs shutdown. ");

        DatabaseAccessor.poolShutdown();
    }

}
