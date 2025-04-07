package com.romibuzi.parquetdiff.junit;

import org.junit.jupiter.api.extension.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Supplier;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(CaptureSystemOut.Extension.class)
public @interface CaptureSystemOut {
    class Extension implements BeforeTestExecutionCallback, AfterTestExecutionCallback, ParameterResolver {
        private final ByteArrayOutputStream testErr = new ByteArrayOutputStream();
        private final ByteArrayOutputStream testOut = new ByteArrayOutputStream();
        private final PrintStream originalErr = System.err;
        private final PrintStream originalOut = System.out;

        @Override
        public void beforeTestExecution(ExtensionContext context) {
            System.setErr(new PrintStream(testErr));
            System.setOut(new PrintStream(testOut));
        }

        @Override
        public void afterTestExecution(ExtensionContext context) {
            System.setErr(originalErr);
            System.setOut(originalOut);
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType().equals(Supplier.class);
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            if (parameterContext.getParameter().isAnnotationPresent(CapturedSystemErr.class)) {
                return (Supplier<String>) testErr::toString;
            } else if (parameterContext.getParameter().isAnnotationPresent(CapturedSystemOut.class)) {
                return (Supplier<String>) testOut::toString;
            }
            return "";
        }
    }
}
