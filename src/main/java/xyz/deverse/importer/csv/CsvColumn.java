package xyz.deverse.importer.csv;

import xyz.deverse.importer.ActionType;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * Annotate a field so that it is put in positional relationship with a csv field of a parsed line during the import
 */
@Target({ FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface CsvColumn {

	int value();

	ActionType actionType() default ActionType.PERSIST;
}
