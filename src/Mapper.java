public class Mapper {

    private static final int mMapperStartingPort = 2001;

    private static final int mReducerStartingPort = 3001;

    private static final int mMainPort = 4001;

    public static void main(String args[]) {
        System.out.println("this is mapper " + args[0]);
    }
}
