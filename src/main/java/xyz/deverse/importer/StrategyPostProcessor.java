package xyz.deverse.importer;

import java.util.Collection;

public interface StrategyPostProcessor<T> {
    default void postProcess(Collection<T> targets) {

    }
}
