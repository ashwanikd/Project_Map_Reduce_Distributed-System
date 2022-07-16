import java.util.List;

public interface ReducerImplInterface {
    public void reduce(String key, List<String> values);

    public Message f(String key, List<String> values);
}
