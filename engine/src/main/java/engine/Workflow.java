package engine;

@FunctionalInterface
public interface Workflow<T> {
    T run(DurableContext context) throws Exception;
}
