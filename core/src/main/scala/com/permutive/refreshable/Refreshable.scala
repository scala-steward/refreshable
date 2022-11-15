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
import fs2.Stream
import fs2.concurrent.SignallingRef
import retry._

import scala.concurrent.duration._

trait Refreshable[F[_], A] { self =>

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
    implicit override protected def functor: Functor[F] = self.functor
    override def get: F[CachedValue[B]] = self.get.map(_.map(f))
    override def cancel: F[Boolean] = self.cancel
    override def restart: F[Boolean] = self.restart
  }

  def mapK[G[_]: Functor](fk: F ~> G): Refreshable[G, A] =
    new Refreshable[G, A] {
      override protected def functor: Functor[G] = implicitly

      override def get: G[CachedValue[A]] = fk(self.get)
      override def cancel: G[Boolean] = fk(self.cancel)
      override def restart: G[Boolean] = fk(self.restart)
    }

}

trait RefreshableUpdates[F[_], A] extends Refreshable[F, A] { self =>

  /** Subscribe to discrete updates of the underlying value
    */
  def updates: Stream[F, CachedValue[A]]

  override def mapK[G[_]: Functor](fk: F ~> G): RefreshableUpdates[G, A] =
    new RefreshableUpdates[G, A] {

      override def updates: Stream[G, CachedValue[A]] =
        self.updates.translate(fk)

      override protected def functor: Functor[G] = implicitly

      override def get: G[CachedValue[A]] = fk(self.get)

      override def cancel: G[Boolean] = fk(self.cancel)

      override def restart: G[Boolean] = fk(self.restart)
    }

  override def map[C](f: A => C): RefreshableUpdates[F, C] =
    new RefreshableUpdates[F, C] {

      implicit override protected def functor: Functor[F] = self.functor

      override def get: F[CachedValue[C]] = self.get.map(_.map(f))
      override def cancel: F[Boolean] = self.cancel
      override def restart: F[Boolean] = self.restart

      override def updates: Stream[F, CachedValue[C]] =
        self.updates.map(_.map(f))
    }
}

object Refreshable {

  /** The default retry policy used when none is specified.
    */
  def defaultPolicy[F[_]: Applicative]: RetryPolicy[F] =
    RetryPolicies
      .constantDelay[F](200.millis)
      .join(RetryPolicies.limitRetries(5))

  def defaultCacheDuration[A]: A => FiniteDuration = _ => 30.seconds

  /** Builder to construct a [[Refreshable]]
    */
  def builder[F[_]: Temporal, A](refresh: F[A]): RefreshableBuilder[F, A] =
    RefreshableBuilder.builder(refresh)

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
    * @param refresh
    *   generate a new value of `A`
    * @param cacheDuration
    *   how long to cache a newly generated value of `A` for, if an effect is
    *   needed to generate this duration it should have occurred in `fa`.
    *   Defaults to [[defaultCacheDuration]] if not specified.
    * @param retryPolicy
    *   a function to derive a configuration object for attempting to retry the
    *   effect of `fa` on failure from the current value of `A`. Defaults to
    *   [[defaultRetryPolicy]] when not specified
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
  class RefreshableBuilder[F[_]: Temporal, A] private[refreshable] (
      val refresh: F[A],
      val cacheDuration: A => FiniteDuration,
      val retryPolicy: A => RetryPolicy[F],
      val onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]],
      val onExhaustedRetries: PartialFunction[Throwable, F[Unit]],
      val onNewValue: Option[(A, FiniteDuration) => F[Unit]],
      val defaultValue: Option[A]
  ) { self =>

    private def copy(
        refresh: F[A] = self.refresh,
        cacheDuration: A => FiniteDuration = self.cacheDuration,
        retryPolicy: A => RetryPolicy[F] = self.retryPolicy,
        onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]] =
          self.onRefreshFailure,
        onExhaustedRetries: PartialFunction[Throwable, F[Unit]] =
          self.onExhaustedRetries,
        onNewValue: Option[(A, FiniteDuration) => F[Unit]] = self.onNewValue,
        defaultValue: Option[A] = self.defaultValue
    ): RefreshableBuilder[F, A] = new RefreshableBuilder[F, A](
      refresh,
      cacheDuration,
      retryPolicy,
      onRefreshFailure,
      onExhaustedRetries,
      onNewValue,
      defaultValue
    )

    def cacheDuration(
        cacheDuration: A => FiniteDuration
    ): RefreshableBuilder[F, A] = copy(cacheDuration = cacheDuration)

    def retryPolicy(
        retryPolicy: A => RetryPolicy[F]
    ): RefreshableBuilder[F, A] = copy(retryPolicy = retryPolicy)

    def retryPolicy(
        retryPolicy: RetryPolicy[F]
    ): RefreshableBuilder[F, A] = copy(retryPolicy = _ => retryPolicy)

    def onRefreshFailure(
        onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]]
    ): RefreshableBuilder[F, A] =
      copy(onRefreshFailure = onRefreshFailure)

    def onExhaustedRetries(
        onExhaustedRetries: PartialFunction[Throwable, F[Unit]]
    ): RefreshableBuilder[F, A] = copy(onExhaustedRetries = onExhaustedRetries)

    def onNewValue(
        onNewValue: (A, FiniteDuration) => F[Unit]
    ): RefreshableBuilder[F, A] = copy(onNewValue = Some(onNewValue))

    def defaultValue(defaultValue: A): RefreshableBuilder[F, A] =
      copy(defaultValue = Some(defaultValue))

    def withUpdates: RefreshableUpdatesBuilder[F, A] =
      new RefreshableUpdatesBuilder[F, A](
        refresh,
        cacheDuration,
        retryPolicy,
        onRefreshFailure,
        onExhaustedRetries,
        onNewValue,
        defaultValue
      )

    def resource: Resource[F, Refreshable[F, A]] = {
      val fa: F[CachedValue[A]] = refresh.map(CachedValue.Success(_))
      for {
        a <- Resource.eval[F, CachedValue[A]](
          defaultValue.fold(fa)(default =>
            fa.handleError(th => CachedValue.Error(default, th))
          )
        )
        store <- Resource.eval(Ref.of[F, CachedValue[A]](a))
        fiberStore <- Resource.eval(
          Ref.of[F, Option[Fiber[F, Throwable, Unit]]](None)
        )
        _ <- runBackground(store, fiberStore)
      } yield RefreshableImpl(store, fiberStore, makeFiber(store))
    }

    protected def makeFiber(
        store: Ref[F, CachedValue[A]]
    )(wait: Deferred[F, Unit]) = (wait.get >> store.get
      .flatMap(a =>
        refreshLoop(
          a.value,
          refresh,
          store.set(_),
          cacheDuration,
          onRefreshFailure,
          onNewValue.getOrElse((_, _) => Applicative[F].unit),
          retryPolicy
        ).handleErrorWith(th => onExhaustedRetries.lift(th).sequence_)
      )).start

    protected def runBackground(
        store: Ref[F, CachedValue[A]],
        fiberStore: Ref[F, Option[Fiber[F, Throwable, Unit]]]
    ): Resource[F, Unit] =
      (for {
        // `.background` means the refresh loop runs in a fiber, but leaving the scope of the `Resource` will cancel
        // it for us. Use the provided callback if a failure occurs in the background fiber, there is no other way to
        // signal a failure from the background.
        wait <- Resource.eval(
          Concurrent[F].deferred[Unit].flatTap(_.complete(()))
        )
        fiber <- Resource.eval(makeFiber(store)(wait))
        _ <- Resource
          .make(fiberStore.set(Some(fiber)))(_ =>
            fiberStore.get.flatMap(_.traverse_(_.cancel))
          )
      } yield ()).uncancelable

    private def refreshLoop(
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
    * @param refresh
    *   generate a new value of `A`
    * @param cacheDuration
    *   how long to cache a newly generated value of `A` for, if an effect is
    *   needed to generate this duration it should have occurred in `fa`.
    *   Defaults to [[defaultCacheDuration]] if not specified.
    * @param retryPolicy
    *   a function to derive a configuration object for attempting to retry the
    *   effect of `fa` on failure from the current value of `A`. Defaults to
    *   [[defaultRetryPolicy]] when not specified
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
  class RefreshableUpdatesBuilder[F[_]: Temporal, A] private[refreshable] (
      refresh: F[A],
      cacheDuration: A => FiniteDuration,
      retryPolicy: A => RetryPolicy[F],
      onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[
        Unit
      ]],
      onExhaustedRetries: PartialFunction[Throwable, F[Unit]],
      onNewValue: Option[(A, FiniteDuration) => F[Unit]],
      defaultValue: Option[A]
  ) extends RefreshableBuilder[F, A](
        refresh,
        cacheDuration,
        retryPolicy,
        onRefreshFailure,
        onExhaustedRetries,
        onNewValue,
        defaultValue
      ) { self =>

    private def copy(
        refresh: F[A] = self.refresh,
        cacheDuration: A => FiniteDuration = self.cacheDuration,
        retryPolicy: A => RetryPolicy[F] = self.retryPolicy,
        onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]] =
          self.onRefreshFailure,
        onExhaustedRetries: PartialFunction[Throwable, F[Unit]] =
          self.onExhaustedRetries,
        onNewValue: Option[(A, FiniteDuration) => F[Unit]] = self.onNewValue,
        defaultValue: Option[A] = self.defaultValue
    ): RefreshableUpdatesBuilder[F, A] = new RefreshableUpdatesBuilder[F, A](
      refresh,
      cacheDuration,
      retryPolicy,
      onRefreshFailure,
      onExhaustedRetries,
      onNewValue,
      defaultValue
    )

    override def cacheDuration(
        cacheDuration: A => FiniteDuration
    ): RefreshableUpdatesBuilder[F, A] = copy(cacheDuration = cacheDuration)

    override def retryPolicy(
        retryPolicy: A => RetryPolicy[F]
    ): RefreshableUpdatesBuilder[F, A] = copy(retryPolicy = retryPolicy)

    override def retryPolicy(
        retryPolicy: RetryPolicy[F]
    ): RefreshableUpdatesBuilder[F, A] = copy(retryPolicy = _ => retryPolicy)

    override def onRefreshFailure(
        onRefreshFailure: PartialFunction[(Throwable, RetryDetails), F[Unit]]
    ): RefreshableUpdatesBuilder[F, A] =
      copy(onRefreshFailure = onRefreshFailure)

    override def onExhaustedRetries(
        onExhaustedRetries: PartialFunction[Throwable, F[Unit]]
    ): RefreshableUpdatesBuilder[F, A] =
      copy(onExhaustedRetries = onExhaustedRetries)

    override def onNewValue(
        onNewValue: (A, FiniteDuration) => F[Unit]
    ): RefreshableUpdatesBuilder[F, A] = copy(onNewValue = Some(onNewValue))

    override def defaultValue(
        defaultValue: A
    ): RefreshableUpdatesBuilder[F, A] =
      copy(defaultValue = Some(defaultValue))

    override def withUpdates: RefreshableUpdatesBuilder[F, A] = self

    override def resource: Resource[F, RefreshableUpdates[F, A]] = {
      val fa: F[CachedValue[A]] = refresh.map(CachedValue.Success(_))
      for {
        a <- Resource.eval[F, CachedValue[A]](
          defaultValue.fold(fa)(default =>
            fa.handleError(th => CachedValue.Error(default, th))
          )
        )
        store <- Resource.eval(SignallingRef.of[F, CachedValue[A]](a))
        fiberStore <- Resource.eval(
          Ref.of[F, Option[Fiber[F, Throwable, Unit]]](None)
        )
        _ <- runBackground(store, fiberStore)
      } yield RefreshableImpl.RefreshableUpdatesImpl(
        store,
        fiberStore,
        makeFiber(store)
      )
    }
  }

  private class RefreshableImpl[F[_]: Concurrent, A] private (
      val store: Ref[F, CachedValue[A]],
      val fiberStore: Ref[F, Option[Fiber[F, Throwable, Unit]]],
      val makeFiber: Deferred[F, Unit] => F[Fiber[F, Throwable, Unit]]
  ) extends Refreshable[F, A] {

    override protected def functor: Functor[F] = implicitly

    override def get: F[CachedValue[A]] = store.get

    override val cancel: F[Boolean] = fiberStore
      .modify {
        case None => None -> false.pure[F]
        case Some(f) =>
          None -> (f.cancel >> store
            .update(v => CachedValue.Cancelled(v.value))
            .as(true))
      }
      .flatten
      .uncancelable

    override val restart: F[Boolean] =
      Concurrent[F].deferred[Unit].flatMap { wait =>
        makeFiber(wait).flatMap { fib =>
          fiberStore.modify {
            case None => Some(fib) -> wait.complete(()).as(true)
            case curr @ Some(_) =>
              curr -> (fib.cancel >> wait.complete(())).as(false)
          }.flatten
        }.uncancelable
      }
  }

  private object RefreshableImpl {
    def apply[F[_]: Concurrent, A](
        store: Ref[F, CachedValue[A]],
        fiberStore: Ref[F, Option[Fiber[F, Throwable, Unit]]],
        makeFiber: Deferred[F, Unit] => F[Fiber[F, Throwable, Unit]]
    ): RefreshableImpl[F, A] = new RefreshableImpl(store, fiberStore, makeFiber)

    private class RefreshableUpdatesImpl[F[_]: Concurrent, A] private (
        store: SignallingRef[F, CachedValue[A]],
        fiberStore: Ref[F, Option[Fiber[F, Throwable, Unit]]],
        makeFiber: Deferred[F, Unit] => F[Fiber[F, Throwable, Unit]]
    ) extends RefreshableImpl[F, A](store, fiberStore, makeFiber)
        with RefreshableUpdates[F, A] {

      override val updates: Stream[F, CachedValue[A]] = store.discrete
    }

    object RefreshableUpdatesImpl {
      def apply[F[_]: Concurrent, A](
          store: SignallingRef[F, CachedValue[A]],
          fiberStore: Ref[F, Option[Fiber[F, Throwable, Unit]]],
          makeFiber: Deferred[F, Unit] => F[Fiber[F, Throwable, Unit]]
      ): RefreshableUpdates[F, A] =
        new RefreshableUpdatesImpl(store, fiberStore, makeFiber)
    }
  }

  private object RefreshableBuilder {
    def builder[F[_]: Temporal, A](fa: F[A]): RefreshableBuilder[F, A] =
      new RefreshableBuilder[F, A](
        refresh = fa,
        cacheDuration = defaultCacheDuration[A],
        retryPolicy = _ => defaultPolicy[F],
        onRefreshFailure = PartialFunction.empty,
        onExhaustedRetries = PartialFunction.empty,
        onNewValue = None,
        defaultValue = None
      )

  }

}
