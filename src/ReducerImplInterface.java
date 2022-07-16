import java.util.List;

public interface ReducerImplInterface {
    public Message reduce(String key, List<String> values);

    public Message f(String arg1, String arg2);
}
