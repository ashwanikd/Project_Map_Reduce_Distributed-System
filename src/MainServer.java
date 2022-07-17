import jdk.nashorn.internal.runtime.ECMAException;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class MainServer {

    private int mPort;

    private ServerSocket mServer;

    private File mFile;

    private FileWriter writer;

    private HashMap<Integer,Integer> mappers;
    private HashMap<Integer,Integer> reducers;

    private boolean check = false;

    private ArrayList<String> messages;

    public MainServer(int port) {
        mPort = port;
        init();
    }

    private void init() {
        mFile = new File("output.csv");
        if (mFile.exists()) {
            mFile.delete();
        }
        try {
            if (mFile.createNewFile()) {
                System.out.println("Successfully created file: " + mFile.getName());
            }
            mappers = new HashMap<>();
            reducers = new HashMap<>();
            messages = new ArrayList<>();
            mServer = new ServerSocket(mPort);
            mServer.setSoTimeout(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startServer() {
        new MainServerThread().start();
        new MonitorThread().start();
    }

    private class MainServerThread implements Runnable {

        Thread thread;

        @Override
        public void run() {
            while (true) {
                try {
                    do {
                        Socket socket = mServer.accept();
                        new SocketThread(socket).start();
                    } while (true);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Server timeout");
                    System.exit(1);
                }
            }
        }

        public void start() {
            thread = new Thread(this,"main server");
            thread.start();
        }
    }

    private class SocketThread implements Runnable {

        private Socket mSocket;

        private DataInputStream mInputStream;

        Thread thread;

        SocketThread(Socket socket) {
            mSocket = socket;
            try {
                mInputStream = new DataInputStream(mSocket.getInputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void start() {
            thread = new Thread(this, "socket thread");
            thread.start();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String message = mInputStream.readUTF();
                    System.out.println("received message :" + message);
                    new HandleMessageTask(message).start();
                } catch (Exception e) {
                    System.out.println("Server " + mSocket.getPort() + " reset");
                    break;
                }
            }
        }
    }

    private class HandleMessageTask implements Runnable {

        String message;

        Thread thread;
        HandleMessageTask(String message) {
            this.message = message;
            thread = new Thread(this,"handle message");
        }

        void start() {
            thread.start();
        }

        @Override
        public void run() {
            try {
                synchronized (mappers) {
                    synchronized (reducers) {
                        writer = new FileWriter(mFile,true);
                        synchronized (writer) {
                            if (message != null) {
                                String[] values = splitThroughSeparator(message, " ");
                                if (values[0].equals("s")) {
                                    int x = Integer.parseInt(values[2]);
                                    if (values[1].equals("map")) {
                                        if (mappers.containsKey(x)) {
                                            mappers.put(x, mappers.get(x) + 1);
                                        } else {
                                            mappers.put(x,1);
                                        }
                                    } else if (values[1].equals("reduce")) {
                                        if (reducers.containsKey(x)) {
                                            reducers.put(x, reducers.get(x) + 1);
                                        } else {
                                            reducers.put(x, 1);
                                        }
                                    }
                                } else if (values[0].equals("c")) {
                                    int x = Integer.parseInt(values[2]);
                                    if (values[1].equals("map")) {
                                        mappers.put(x, mappers.get(x) - 1);
                                    } else if (values[1].equals("reduce")) {
                                        reducers.put(x, reducers.get(x) - 1);
                                    }
                                } else {
                                    writer.write(message);
                                }
                                System.out.println("Successfully write message : " + message);
                            }
                        }
                    }
                    writer.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class MonitorThread implements Runnable {

        Thread thread;

        MonitorThread() {
            thread = new Thread(this,"monitor thread");
        }

        void start() {
            thread.start();
        }

        @Override
        public void run() {
            while (!allFinished() && !mServer.isClosed()) {

            }
            try {
                if (!mServer.isClosed()) {
                    mServer.close();
                }
                writer.close();
                System.out.println("Program completed Successfully");
                System.exit(0);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean allFinished() {
        synchronized (mappers) {
            synchronized (reducers) {
                if (!mappers.isEmpty() && !reducers.isEmpty()) {
                    Set<Integer> keys = mappers.keySet();
                    Iterator it = keys.iterator();
                    int key;
                    while (it.hasNext()) {
                        key = (Integer) it.next();
                        if (mappers.get(key) != 0) {
                            return false;
                        }
                    }
                    keys = reducers.keySet();
                    it = keys.iterator();
                    while (it.hasNext()) {
                        key = (Integer) it.next();
                        if (reducers.get(key) != 0) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
                return true;
            }
        }
    }

    public static String[] splitThroughSeparator(String key, String separator) {
        String[] result;
        ArrayList<String> resultList = new ArrayList<String>();
        int x = key.indexOf(separator);
        while (x > 0) {
            resultList.add(key.subSequence(0,x).toString());
            key = key.subSequence(x + 1,key.length()).toString();
            x = key.indexOf(separator);
        }
        if (!key.equals("") && key != null) {
            resultList.add(key);
        }
        result = new String[resultList.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = resultList.get(i);
        }
        return result;
    }
}
