public class Main {

    private static final int mMapperStartingPort = 2001;

    private static final int mReducerStartingPort = 3001;

    private static final int mMainPort = 4001;

    private static int mNoOfReducers;

    public static void main(String[] args) {
        System.out.println("Hello world!");
        for (String arg : args) {
            System.out.println(arg);
        }
    }
}