package xyz.deverse.importer.pipeline;

import java.util.Collection;
import java.util.Optional;

import xyz.deverse.importer.ImportLine;


public interface Pipeline<T> extends Runnable {

	interface Stage extends Runnable {}

	<S extends Stage> Optional<S> getStageByType(Class<S> stageType);

	Collection<T> getOutput();
		
	Collection<? extends ImportLine> getErrors();
}

