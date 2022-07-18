/**
 * invokes the Main Server
 */
public class Main {

    public static final int mMapperStartingPort = 2001;

    public static final int mReducerStartingPort = 3001;

    public static final int mMainPort = 4001;

    public static int mNoOfReducers;

    public static void main(String[] args) {
        MainServer mainServer = new MainServer(mMainPort);
        mainServer.startServer();
    }
}