package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor<T> implements InvocationHandler {

  private final Clock clock;
  private final T delegate;
  private final ProfilingState profilingState;

  // TODO: You will need to add more instance fields and constructor arguments to this class.
  ProfilingMethodInterceptor(Clock clock, T delegate, ProfilingState profilingState) {
    this.clock = Objects.requireNonNull(clock);
    this.delegate = delegate;
    this.profilingState = profilingState;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // TODO: This method interceptor should inspect the called method to see if it is a profiled
    //       method. For profiled methods, the interceptor should record the start time, then
    //       invoke the method using the object that is being profiled. Finally, for profiled
    //       methods, the interceptor should record how long the method call took, using the
    //       ProfilingState methods.

    Object result = null;

    if (method.isAnnotationPresent(Profiled.class)) {
      Instant startTime = clock.instant();

      Throwable exception = null;
      try {
        result = method.invoke(delegate, args);
      } catch (Exception e) {
        exception = e;
      }
      Duration elapsed = Duration.between(startTime, clock.instant());

      if (elapsed.isNegative()) {
        throw new IllegalArgumentException();
      }

      profilingState.record(delegate.getClass(), method, elapsed);

      if(exception != null) {
        throw new Throwable(exception.getCause().getMessage());
      }
    } else {
      result = method.invoke(delegate, args);

    }
    return result;
  }
}
