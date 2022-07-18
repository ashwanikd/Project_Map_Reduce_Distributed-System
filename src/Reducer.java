/**
 * invokes the Reducer server
 */
public class Reducer {
    public static final int mMapperStartingPort = 2001;

    public static final int mReducerStartingPort = 3001;

    public static final int mMainPort = 4001;

    public static void main(String args[]) {
        DistributedReducer reducer = new DistributedReducer();
        reducer.startServer();
    }
}
