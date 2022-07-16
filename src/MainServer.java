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

    private Scanner scanner;

    private DataInputStream mInputStream;

    private FileWriter writer;

    private HashMap<Integer,Boolean> mappers;
    private HashMap<Integer,Boolean> reducers;

    private boolean check = false;

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
                writer = new FileWriter(mFile,true);
            }
            mServer = new ServerSocket(mPort);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mappers = new HashMap<>();
            reducers = new HashMap<>();
        }
    }

    public void startServer() {
        new MainServerThread().start();
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
                    } while (allFinished());
                } catch (Exception e) {
                    e.printStackTrace();
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
                    if (message != null) {
                        String[] values = message.split(" ");
                        if (values[0].equals("s")) {
                            if (values[1].equals("map")) {
                                mappers.put(Integer.parseInt(values[2]),true);
                            } else if (values[1].equals("reduce")) {
                                reducers.put(Integer.parseInt(values[2]),true);
                            }
                        } else if (values[0].equals("c")) {
                            if (values[1].equals("map")) {
                                mappers.put(Integer.parseInt(values[2]),true);
                            } else if (values[1].equals("reduce")) {
                                reducers.put(Integer.parseInt(values[2]),true);
                            }
                        } else {
                            if (!check) {
                                writer.write(message);
                                check = true;
                            } else {
                                writer.write("\n" + message);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean allFinished() {
        Set<Integer> keys = mappers.keySet();
        Iterator it = keys.iterator();
        int key;
        while (it.hasNext()) {
            key = (Integer) it.next();
            if (mappers.get(key)) {
                return false;
            }
        }
        keys = reducers.keySet();
        it = keys.iterator();
        while (it.hasNext()) {
            key = (Integer) it.next();
            if (mappers.get(key)) {
                return false;
            }
        }
        return true;
    }
}
