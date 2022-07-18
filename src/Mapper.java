/**
 * invokes the main Mapper server the passing arguments should be the input file
 */
public class Mapper {

    public static final int mMapperStartingPort = 2001;

    public static final int mReducerStartingPort = 3001;

    public static final int mMainPort = 4001;

    public static void main(String args[]) {
        DistributedMapper mapper = new DistributedMapper(args[0], Integer.parseInt(args[1]));
        mapper.runServer();
    }
}
