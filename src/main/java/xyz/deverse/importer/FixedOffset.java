package xyz.deverse.importer;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * Annotate a field so that it is put in positional relationship with a csv field of a parsed line during the import
 */
@Target({ FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface FixedOffset {

    int start();

    int end();

}
