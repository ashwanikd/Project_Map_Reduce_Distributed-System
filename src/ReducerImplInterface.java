import java.util.ArrayList;
import java.util.List;

public interface ReducerImplInterface {
    public void reduce(String key, ArrayList<String> values);

    public Message f(String key, ArrayList<String> values);
}
