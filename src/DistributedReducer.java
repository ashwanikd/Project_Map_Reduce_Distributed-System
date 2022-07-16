import java.net.ServerSocket;
import java.util.ArrayList;

public class DistributedReducer {

    private int mPort;

    private ServerSocket mServer;

    ArrayList<String> messages; // message queue

    public DistributedReducer(int port) {

    }

    // initialize variables
    private void init() {

    }

    // start server listening on port mPort
    private void startServer() {

        // start both threads after starting server on listeneing on port

    }

    private class ServerProcess implements Runnable {
        @Override
        public void run() {
            synchronized (messages) {
                while (true) {
                    // pop messages from messages and reply or process
                }
            }
        }
    }

    private class ServerMessageProcess implements Runnable {
        @Override
        public void run() {
            synchronized (messages) {
                while (true) {
                    // listen the server and add messages to messages queue
                }
            }
        }
    }
}
