package com.github.bingoohuang.mtcp.util;

import com.github.bingoohuang.mtcp.LightConfig;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that reflectively sets bean properties on a target object.
 *
 * @author Brett Wooldridge
 */
@Slf4j
public final class PropertyElf {
    private static final Pattern GETTER_PATTERN = Pattern.compile("(get|is)[A-Z].+");

    public static void setTargetFromProperties(final Object target, final Properties properties) {
        if (target == null || properties == null) {
            return;
        }

        List<Method> methods = Arrays.asList(target.getClass().getMethods());
        properties.forEach((key, value) -> {
            String keyString = key.toString();
            if (target instanceof LightConfig && keyString.startsWith("dataSource.")) {
                ((LightConfig) target).addDataSourceProperty(keyString.substring("dataSource.".length()), value);
            } else {
                setProperty(target, keyString, value, methods);
            }
        });
    }

    /**
     * Get the bean-style property names for the specified object.
     *
     * @param targetClass the target object
     * @return a set of property names
     */
    public static Set<String> getPropertyNames(final Class<?> targetClass) {
        HashSet<String> set = new HashSet<>();
        Matcher matcher = GETTER_PATTERN.matcher("");
        for (Method method : targetClass.getMethods()) {
            String name = method.getName();
            if (method.getParameterTypes().length == 0 && matcher.reset(name).matches()) {
                name = name.replaceFirst("(get|is)", "");
                try {
                    if (targetClass.getMethod("set" + name, method.getReturnType()) != null) {
                        name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
                        set.add(name);
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        }

        return set;
    }

    public static Object getProperty(final String propName, final Object target) {
        try {
            // use the english locale to avoid the infamous turkish locale bug
            String capitalized = "get" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH) + propName.substring(1);
            Method method = target.getClass().getMethod(capitalized);
            return method.invoke(target);
        } catch (Exception e) {
            try {
                String capitalized = "is" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH) + propName.substring(1);
                Method method = target.getClass().getMethod(capitalized);
                return method.invoke(target);
            } catch (Exception e2) {
                return null;
            }
        }
    }

    public static Properties copyProperties(final Properties props) {
        Properties copy = new Properties();
        props.forEach((key, value) -> copy.setProperty(key.toString(), value.toString()));
        return copy;
    }

    private static void setProperty(final Object target, final String propName, final Object propValue, final List<Method> methods) {
        // use the english locale to avoid the infamous turkish locale bug
        String methodName = "set" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH) + propName.substring(1);
        Method writeMethod = methods.stream().filter(m -> m.getName().equals(methodName) && m.getParameterCount() == 1).findFirst().orElse(null);

        if (writeMethod == null) {
            String methodName2 = "set" + propName.toUpperCase(Locale.ENGLISH);
            writeMethod = methods.stream().filter(m -> m.getName().equals(methodName2) && m.getParameterCount() == 1).findFirst().orElse(null);
        }

        if (writeMethod == null) {
            log.error("Property {} does not exist on target {}", propName, target.getClass());
            throw new RuntimeException(String.format("Property %s does not exist on target %s", propName, target.getClass()));
        }

        try {
            Class<?> paramClass = writeMethod.getParameterTypes()[0];
            if (paramClass == int.class) {
                writeMethod.invoke(target, Integer.parseInt(propValue.toString()));
            } else if (paramClass == long.class) {
                writeMethod.invoke(target, Long.parseLong(propValue.toString()));
            } else if (paramClass == boolean.class || paramClass == Boolean.class) {
                writeMethod.invoke(target, Boolean.parseBoolean(propValue.toString()));
            } else if (paramClass == String.class) {
                writeMethod.invoke(target, propValue.toString());
            } else {
                writeMethod.invoke(target, propValue);
            }
        } catch (Exception e) {
            log.error("Failed to set property {} on target {}", propName, target.getClass(), e);
            throw new RuntimeException(e);
        }
    }
}
