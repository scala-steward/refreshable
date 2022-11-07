/*
 * Copyright 2022 Permutive
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.permutive.refreshable

import cats.effect._
import cats.effect.syntax.all._
import cats.syntax.all._
import cats.{~>, Applicative, Functor}
import retry._

import scala.concurrent.duration._

trait Refreshable[F[_], A] { outer =>

  implicit protected def functor: Functor[F]

  /** Get the unwrapped value of `A`
    */
  def value: F[A] = get.map(_.value)

  /** Get the value of `A` wrapped in a status
    */
  def get: F[CachedValue[A]]

  /** Cancel refreshing
    *
    * @return
    *   boolean status of whether refreshing was stopped (false if it is already
    *   stopped)
    */
  def cancel: F[Boolean]

  /** Restart refreshing
    *
    * @return
    *   boolean status of whether refreshing was restarted (false if is already
    *   started)
    */
  def restart: F[Boolean]

  def map[B](f: A => B): Refreshable[F, B] = new Refreshable[F, B] {
    implicit override protected def functor: Functor[F] = outer.functor
    override def get: F[CachedValue[B]] = outer.get.map(_.map(f))
    override def cancel: F[Boolean] = outer.cancel
    override def restart: F[Boolean] = outer.restart
  }

  def mapK[G[_]: Functor](fk: F ~> G): Refreshable[G, A] =
    Refreshable.mapK(this)(fk)
}

object Refreshable {

  def defaultPolicy[F[_]: Applicative]: RetryPolicy[F] =
    RetryPolicies
      .constantDelay[F](200.millis)
      .join(RetryPolicies.limitRetries(5))

  /** Caches a single instance of type `A` for a period of time before
    * refreshing it automatically.
    *
    * The time between refreshes is dynamic and based on the value of each `A`
    * itself. This is similar to `RefreshableEffect` except that only exposes a
    * fixed refresh frequency.
    *
    * An old value is only made unavailable _after_ a new value has been
    * acquired. This means that the time each value is exposed for is
    * `cacheDuration` plus the time to evaluate `fa`.
    *
    * @param fa
    *   generate a new value of `A`
    * @param cacheDuration
    *   how long to cache a newly generated value of `A` for, if an effect is
    *   needed to generate this duration it should have occurred in `fa`
    * @param onRefreshFailure
    *   what to do when an attempt to refresh the value fails, `fa` will be
    *   retried according to `retryPolicy`
    * @param onExhaustedRetries
    *   what to do if retrying to refresh the value fails. The refresh fiber
    *   will have failed at this point and the value will grow stale. It is up
    *   to user handle this failure, as they see fit, in their application
    * @param onNewValue
    *   a callback invoked whenever a new value is generated, the
    *   [[scala.concurrent.duration.FiniteDuration]] is the period that will be
    *   waited before the next new value
    * @param defaultValue
    *   an optional default value to use when initialising the resource, if the
    *   call to `fa` fails. This will prevent the constructor from failing
    *   during startup
    * @param retryPolicy
    *   an optional configuration object for attempting to retry the effect of
    *   `fa` on failure. When no value is supplied this defaults to
    *   [[defaultPolicy]]
    */
  def resource[F[_]: Temporal, A](
      refresh: F[A],
      cacheDuration: A => FiniteDuration,
      onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]],
      onExhaustedRetries: PartialFunction[Throwable, F[Unit]],
      onNewValue: Option[(A, FiniteDuration) => F[Unit]] = None,
      defaultValue: Option[A] = None,
      retryPolicy: Option[RetryPolicy[F]] = None
  ): Resource[F, Refreshable[F, A]] = {
    val faCv: F[CachedValue[A]] = refresh.map(CachedValue.Success(_))

    for {
      a <- Resource.eval(
        defaultValue.fold(faCv)(default =>
          faCv.handleError(th => CachedValue.Error(default, th))
        )
      )
      ref <- Resource.eval(Ref.of[F, CachedValue[A]](a))
      rv <- impl(
        refresh,
        cacheDuration,
        onRefreshFailure,
        onExhaustedRetries,
        onNewValue,
        ref.get,
        ref.set,
        ref.update,
        retryPolicy.map((_: A) => _)
      )
    } yield rv
  }

  /** Caches a single instance of type `A` for a period of time before
    * refreshing it automatically.
    *
    * The time between refreshes is dynamic and based on the value of each `A`
    * itself. This is similar to `RefreshableEffect` except that only exposes a
    * fixed refresh frequency.
    *
    * As well as the time between refreshes, the retry policy is also dynamic
    * and based on the value for `A`. This allows you to configure the policy
    * based on when `A` is going to expire.
    *
    * You can use the `cacheDuration` and `retryPolicy` together to eagerly
    * fetch a new value for `A` using the calculated cache duration minus some
    * duration to allow for retries and then set the retry policy to retry
    * throughout that duration.
    *
    * An old value is only made unavailable _after_ a new value has been
    * acquired. This means that the time each value is exposed for is
    * `cacheDuration` plus the time to evaluate `fa`.
    *
    * @param fa
    *   generate a new value of `A`
    * @param cacheDuration
    *   how long to cache a newly generated value of `A` for, if an effect is
    *   needed to generate this duration it should have occurred in `fa`
    * @param retryPolicy
    *   a function to derive a configuration object for attempting to retry the
    *   effect of `fa` on failure from the current value of `A`.
    * @param onRefreshFailure
    *   what to when an attempt to refresh the value fails, `fa` will be retried
    *   according to `retryPolicy`
    * @param onExhaustedRetries
    *   what to do if retrying to refresh the value fails. The refresh fiber
    *   will have failed at this point and the value will grow stale. It is up
    *   to user handle this failure, as they see fit, in their application
    * @param onNewValue
    *   a callback invoked whenever a new value is generated, the
    *   [[scala.concurrent.duration.FiniteDuration]] is the period that will be
    *   waited before the next new value
    * @param defaultValue
    *   an optional default value to use when initialising the resource, if the
    *   call to `fa` fails. This will prevent the constructor from failing
    *   during startup
    */
  def derivedRetry[F[_]: Temporal, A](
      refresh: F[A],
      cacheDuration: A => FiniteDuration,
      retryPolicy: A => RetryPolicy[F],
      onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]],
      onExhaustedRetries: PartialFunction[Throwable, F[Unit]],
      onNewValue: Option[(A, FiniteDuration) => F[Unit]] = None,
      defaultValue: Option[A] = None
  ): Resource[F, Refreshable[F, A]] = {
    val faCv: F[CachedValue[A]] = refresh.map(CachedValue.Success(_))

    for {
      a <- Resource.eval(
        defaultValue.fold(faCv)(default =>
          faCv.handleError(th => CachedValue.Error(default, th))
        )
      )
      ref <- Resource.eval(Ref.of[F, CachedValue[A]](a))
      rv <- impl(
        refresh,
        cacheDuration,
        onRefreshFailure,
        onExhaustedRetries,
        onNewValue,
        ref.get,
        ref.set,
        ref.update,
        Some(retryPolicy)
      )
    } yield rv
  }

  private[refreshable] def impl[F[_]: Temporal, A](
      refresh: F[A],
      cacheDuration: A => FiniteDuration,
      onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]],
      onExhaustedRetries: PartialFunction[Throwable, F[Unit]],
      onNewValue: Option[(A, FiniteDuration) => F[Unit]] = None,
      getValue: F[CachedValue[A]],
      setValue: CachedValue[A] => F[Unit],
      updateValue: (CachedValue[A] => CachedValue[A]) => F[Unit],
      retryPolicy: Option[A => RetryPolicy[F]]
  ): Resource[F, Refreshable[F, A]] = {
    val newValueHook: (A, FiniteDuration) => F[Unit] =
      onNewValue.getOrElse((_, _) => Applicative[F].unit)

    val rp =
      retryPolicy.getOrElse((_: A) => defaultPolicy)

    def makeFiber(wait: Deferred[F, Unit]) = (wait.get >> getValue
      .flatMap(a =>
        refreshLoop[F, A](
          a.value,
          refresh,
          setValue,
          cacheDuration,
          onRefreshFailure,
          newValueHook,
          rp
        ).handleErrorWith(th => onExhaustedRetries.lift(th).sequence_)
      )).start

    (for {
      // `.background` means the refresh loop runs in a fiber, but leaving the scope of the `Resource` will cancel
      // it for us. Use the provided callback if a failure occurs in the background fiber, there is no other way to
      // signal a failure from the background.
      wait <- Resource.eval(
        Concurrent[F].deferred[Unit].flatTap(_.complete(()))
      )
      fiber <- Resource.eval(makeFiber(wait))
      fiberRef <- Resource
        .make(Ref.of[F, Option[Fiber[F, Throwable, Unit]]](Some(fiber)))(
          _.get.flatMap(_.traverse_(_.cancel))
        )
    } yield new Refreshable[F, A] {
      override protected val functor: Functor[F] = implicitly

      override val get: F[CachedValue[A]] = getValue

      override val cancel: F[Boolean] = fiberRef
        .modify {
          case None => None -> false.pure[F]
          case Some(f) =>
            None -> (f.cancel >> updateValue(v =>
              CachedValue.Cancelled(v.value)
            )
              .as(true))
        }
        .flatten
        .uncancelable

      override val restart: F[Boolean] =
        Concurrent[F].deferred[Unit].flatMap { wait =>
          makeFiber(wait).flatMap { fib =>
            fiberRef.modify {
              case None => Some(fib) -> wait.complete(()).as(true)
              case curr @ Some(_) =>
                curr -> (fib.cancel.start >> wait.complete(())).as(false)
            }.flatten
          }.uncancelable
        }

    }).uncancelable
  }

  private def refreshLoop[F[_]: Temporal, A](
      initialA: A,
      fa: F[A],
      set: CachedValue[A] => F[Unit],
      cacheDuration: A => FiniteDuration,
      onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]],
      onNewValue: (A, FiniteDuration) => F[Unit],
      retryPolicy: A => RetryPolicy[F]
  ): F[Unit] = {
    def innerLoop(currentA: A): F[Unit] = {
      val faError = fa.onError { case th =>
        set(CachedValue.Error(currentA, th))
      }

      val retryFa =
        retryingOnAllErrors(
          policy = retryPolicy(currentA),
          (th: Throwable, details) =>
            onRefreshFailure.lift((th, details)).sequence_
        )(faError)

      val duration = cacheDuration(currentA)
      for {
        _ <- onNewValue(currentA, duration)
        _ <- Temporal[F].sleep(duration)
        // Note the old value is only removed from the `Ref` after we have acquired a new value.
        // We could remove the old value instantly if this implementation also used a `Deferred` and consumers block on
        // the empty deferred during acquisition of a new value. This would lead to edge cases that would be unpleasant
        // though; for example we'd need to handle the case of failing to acquire a new value ensuring consumers do not
        // block on an empty deferred forever.
        newA <- retryFa
        _ <- set(CachedValue.Success(newA))
        _ <- innerLoop(newA)
      } yield ()
    }

    innerLoop(initialA)
  }

  private def mapK[F[_], G[_]: Functor, A](
      self: Refreshable[F, A]
  )(fk: F ~> G) = new Refreshable[G, A] {
    override protected def functor: Functor[G] = implicitly

    override def get: G[CachedValue[A]] = fk(self.get)
    override def cancel: G[Boolean] = fk(self.cancel)
    override def restart: G[Boolean] = fk(self.restart)
  }
}
