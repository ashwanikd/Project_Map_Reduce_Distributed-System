import java.util.List;

public interface ReducerImplInterface {
    public Message reduce(String key, List<String> values);
}
