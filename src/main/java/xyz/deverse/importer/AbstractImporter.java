package xyz.deverse.importer;

import java.util.*;

import javax.validation.ConstraintViolation;
import javax.validation.ValidationException;
import javax.validation.Validator;

import xyz.deverse.importer.generic.ImportTag;
import xyz.deverse.importer.misc.ParameterizedClassTypeResolver;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ReflectionUtils;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;

import lombok.Getter;

public abstract class AbstractImporter<T, S extends ImportLine> implements Importer<T, S> {

	static final Converter<String, String> caseConverter = CaseFormat.UPPER_UNDERSCORE.converterTo(CaseFormat.LOWER_HYPHEN);

	/**
	 * Flags accepted as boolean for importer operatino
	 */
	public static final Set<String> BOOLEAN_FLAGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("oui", "yes", "y", "o")));

	@Autowired
	private Validator validator;

	@Getter
	protected List<? extends ImportTag> importTags;

	@Getter
	protected Class<T> nodeType;

	@Getter
	protected Class<? extends ImportLine> lineType;

	protected ImportStrategy<T, S> strategy;

	@SuppressWarnings("unchecked")
	protected AbstractImporter(ImportTag... importTags) {
		this.nodeType = (Class<T>) ParameterizedClassTypeResolver.getTypeParameters(getClass())[0];
		this.lineType = (Class<? extends ImportLine>) ParameterizedClassTypeResolver.getTypeParameters(getClass())[1];
		this.importTags = Collections.unmodifiableList(Arrays.asList(importTags));
	}

	@Override
	public String getPublisherUrl() {
		String res = "";
		for (ImportTag importTag : importTags) {
			res = res + "/" + caseConverter.convert(importTag.name());
		}
		return "/topic/import" + res;
	}

	@Override
	public void process(ImportStrategy<T, S> strategy) {
		this.strategy = strategy;
		preProcess();
		strategy.getLineProcessors().add(s -> {
			reinitialize();
			this.onParseLine((S)s);
		});
		strategy.parse();
	}

	@Override
	public Boolean isMatching(List<? extends ImportTag> importTags) {
		if (this.importTags.containsAll(importTags)) {
			return true;
		}

		return false;
	}

	protected void reinitialize() {
		// Do nothing by default. Use this method to clear instance state each time a new line is parsed
	}

	@SuppressWarnings("unchecked")
	protected Collection<S> getImportedLines() {
		if (strategy == null) {
			return Collections.emptyList();
		}
		return (Collection<S>) strategy.getImportedLines();
	}

	private void performValidation(String parentPropertyPath, T node, int depth) {
		Set<ConstraintViolation<T>> violations = validator.validate(node);
		if (!violations.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			for (ConstraintViolation<T> violation : violations) {
				if (sb.length() > 0) {
					sb.append("; ");
				}
				Object invalidValue = violation.getInvalidValue();
				String loggableInvalidValue = "null";
				if (invalidValue != null) {
					loggableInvalidValue = node.getClass().isAssignableFrom(invalidValue.getClass()) ? invalidValue.getClass().getSimpleName() : invalidValue.toString();
				}
				String propertyPath = String.join(".", parentPropertyPath, node.getClass().getSimpleName(), violation.getPropertyPath().toString());
				if (propertyPath.startsWith(".")) {
					propertyPath = propertyPath.substring(1, propertyPath.length());
				}
				// Only consider this a constraint ValidationException if the specified depth is not reached, by counting the property path separators '.'
				int separatorsCount = propertyPath.split("\\.").length - 1;
				if (separatorsCount - 1 < depth) {
					sb.append(propertyPath + " = " + loggableInvalidValue + ", " + violation.getMessage());
				}
			}
			if (sb.length() > 0) {
				throw new ValidationException(sb.toString());
			}
		}
	}

	protected void validate(T node, int depth) throws ValidationException {
		performValidation("", node, depth);
		final BeanWrapper nodeBean = new BeanWrapperImpl(node);
		ReflectionUtils.doWithFields(node.getClass(), f -> {
			if (node.getClass().isAssignableFrom(f.getType())) {
				T value = (T) nodeBean.getPropertyValue(f.getName());
				if (value != null) {
					performValidation(node.getClass().getSimpleName(), value, depth);
				}
			}
		});
	}
}
