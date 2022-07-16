import java.util.List;

/**
 * @version 1.0
 * implement your Reducer code inside the class
 */
public class ReducerImplementation implements ReducerImplInterface{

    MessageList mMessageList = new MessageList();

    @Override
    public void reduce(String key, List<String> values) {

    }

    @Override
    public Message f(String key, List<String> values) {
        return null;
    }
}
