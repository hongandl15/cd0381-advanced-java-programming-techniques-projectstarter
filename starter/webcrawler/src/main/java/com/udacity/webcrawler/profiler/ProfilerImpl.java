package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

  private final Clock clock;
  private final ProfilingState state = new ProfilingState();
  private final ZonedDateTime startTime;

  @Inject
  ProfilerImpl(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
    this.startTime = ZonedDateTime.now(clock);
  }

  @Override
  public <T> T wrap(Class<T> klass, T delegate) {
    validateWrapArguments(klass, delegate);

    return createProxyInstance(klass, delegate);
  }

  private <T> void validateWrapArguments(Class<T> klass, T delegate) {
    Objects.requireNonNull(klass, "Class type cannot be null");
    Objects.requireNonNull(delegate, "Delegate instance cannot be null");

    boolean hasProfiledMethod = Arrays.stream(klass.getMethods())
            .anyMatch(method -> method.isAnnotationPresent(Profiled.class));

    if (!hasProfiledMethod) {
      throw new IllegalArgumentException("No methods annotated with @Profiled");
    }
  }

  private <T> T createProxyInstance(Class<T> klass, T delegate) {
    return (T) Proxy.newProxyInstance(
            ProfilerImpl.class.getClassLoader(),
            new Class[]{klass},
            new ProfilingMethodInterceptor(clock, delegate, state)
    );
  }

  @Override
  public void writeData(Path path) {
    Objects.requireNonNull(path, "Path cannot be null");

    try (Writer writer = Files.newBufferedWriter(path)) {
      writeData(writer);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write profiling data to file", e);
    }
  }

  @Override
  public void writeData(Writer writer) throws IOException {
    Objects.requireNonNull(writer, "Writer cannot be null");

    writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
    writer.write(System.lineSeparator());
    state.write(writer);
    writer.write(System.lineSeparator());
  }
}