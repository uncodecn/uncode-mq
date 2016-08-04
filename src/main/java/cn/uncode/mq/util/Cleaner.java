package cn.uncode.mq.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class Cleaner extends PhantomReference<Object> {
	
	// Dummy reference queue, needed because the PhantomReference constructor
    // insists that we pass a queue.  Nothing will ever be placed on this queue
    // since the reference handler invokes cleaners explicitly.
    //
    private static final ReferenceQueue<Object> dummyQueue = new ReferenceQueue<Object>();

    // Doubly-linked list of live cleaners, which prevents the cleaners
    // themselves from being GC'd before their referents
    //
    static private Cleaner first = null;

    private Cleaner
        next = null,
        prev = null;

    private static synchronized Cleaner add(Cleaner cl) {
        if (first != null) {
            cl.next = first;
            first.prev = cl;
        }
        first = cl;
        return cl;
    }

    private static synchronized boolean remove(Cleaner cl) {

        // If already removed, do nothing
        if (cl.next == cl)
            return false;

        // Update list
        if (first == cl) {
            if (cl.next != null)
                first = cl.next;
            else
                first = cl.prev;
        }
       if (cl.next != null)
           cl.next.prev = cl.prev;
       if (cl.prev != null)
           cl.prev.next = cl.next;

       // Indicate removal by pointing the cleaner to itself
       cl.next = cl;
       cl.prev = cl;
       return true;

   }

   private final Runnable thunk;

   private Cleaner(Object referent, Runnable thunk) {
       super(referent, dummyQueue);
       this.thunk = thunk;
   }

   /**
    * Creates a new cleaner.
    *
    * @param  thunk
    *         The cleanup code to be run when the cleaner is invoked.  The
    *         cleanup code is run directly from the reference-handler thread,
    *         so it should be as simple and straightforward as possible.
    *
    * @return  The new cleaner
    */
   public static Cleaner create(Object ob, Runnable thunk) {
       if (thunk == null)
           return null;
       return add(new Cleaner(ob, thunk));
   }

   /**
    * Runs this cleaner, if it has not been run before.
    */
   public void clean() {
       if (!remove(this))
           return;
       try {
           thunk.run();
       } catch (final Throwable x) {
           AccessController.doPrivileged(new PrivilegedAction<Void>() {
           public Void run() {
               if (System.err != null)
                   new Error("Cleaner terminated abnormally", x)
                       .printStackTrace();
               System.exit(0);
               return null;
           }});
       }
   }

}
