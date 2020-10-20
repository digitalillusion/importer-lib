package xyz.deverse.importer.misc;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Objects;

public class ParameterizedClassTypeResolver {

	public static Type[] getTypeParameters(Class<?> parameterizedClass) {
		Type genericSuperClass = parameterizedClass;

		while (!Objects.equals(Object.class, genericSuperClass)) {
			if ((genericSuperClass instanceof ParameterizedType)) {
				Type[] typeArguments = ((ParameterizedType) genericSuperClass).getActualTypeArguments();
				return Arrays.stream(typeArguments).map(typeArg -> {
					if (ParameterizedType.class.isAssignableFrom(typeArg.getClass())) {
						return ((ParameterizedType) typeArg).getRawType();
					}
					return typeArg;
				}).toArray(Type[]::new);
			} else {
				genericSuperClass = ((Class<?>) genericSuperClass).getGenericSuperclass();
			}
		}
		return new Class[0];
	}
}
