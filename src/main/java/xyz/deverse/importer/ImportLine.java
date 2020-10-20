package xyz.deverse.importer;


import org.slf4j.event.Level;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public interface ImportLine {

	Collection<?> getNodes();

	Set<Long> getExcludedIds();

	ActionType getActionType();

	AtomicInteger getSaveDepth();

	String getGroup();

	List<String> getGroups();

	int getIndex();

	int getIndexInGroup();

	int getCount();

	Level getSeverity();

	String getMessage();

	ImportLine withoutNodes();
}