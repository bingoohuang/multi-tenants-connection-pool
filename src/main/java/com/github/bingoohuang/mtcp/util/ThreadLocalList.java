package com.github.bingoohuang.mtcp.util;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

public class ThreadLocalList<T extends ConcurrentBag.IConcurrentBagEntry> {
   public interface Holder<T> {
      T get();
   }

   public static class DirectHolder<T> implements Holder<T> {
      private final T item;

      public DirectHolder(T item) {
         this.item = item;
      }

      @Override public T get() {
         return item;
      }
   }

   public static class WeakReferenceHolder<T> implements Holder<T> {
      private final WeakReference<T> item;

      public WeakReferenceHolder(T bagEntry) {
         this.item = new WeakReference<>(bagEntry);
      }

      @Override public T get() {
         return item.get();
      }
   }

   public interface HolderFactory<T> {
      Holder<T> createHolder(T bagEntry);
   }

   private final ThreadLocal<List<Holder<T>>> threadList;
   private final HolderFactory<T> holderFactory;

   @SuppressWarnings("unchecked")
   public ThreadLocalList() {
      final boolean weakThreadLocals = useWeakThreadLocals();
      if (weakThreadLocals) {
         this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
         this.holderFactory = (bagEntry) -> new WeakReferenceHolder(bagEntry);
      } else {
         this.threadList = ThreadLocal.withInitial(() -> new FastList<>(DirectHolder.class, 16));
         this.holderFactory = (bagEntry) -> new DirectHolder(bagEntry);
      }
   }

   public void add(T bagEntry) {
      threadList.get().add(holderFactory.createHolder(bagEntry));
   }

   public T get() {
      final List<Holder<T>> list = threadList.get();
      for (int i = list.size() - 1; i >= 0; i--) {
         final Holder<T> entry = list.remove(i);
         final T bagEntry = entry.get();
         if (bagEntry != null && bagEntry.compareAndSet(ConcurrentBag.IConcurrentBagEntry.STATE_FREE, ConcurrentBag.IConcurrentBagEntry.STATE_USING)) {
            return bagEntry;
         }
      }

      return null;
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    *
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals() {
      try {
         if (System.getProperty("com.github.bingoohuang.mtcp.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.github.bingoohuang.mtcp.useWeakReferences");
         }

         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
      } catch (SecurityException se) {
         return true;
      }
   }
}
