import scala.reflect.ClassTag

object Util {

  implicit class Long2Int(x: Long) {
    def clip(): Int = {
      (x % (1 << 30)).toInt
    }
  }

  def binarySearch(begin: Long, end: Long, predicate: Long => Boolean): Long = {
    var b = begin
    var e = end
    while (begin < end) {
      val m = b + (e - b) / 2
      if (predicate(m)) {
        b = m + 1
      } else {
        e = m
      }
    }
    b
  }

  /**
    * Constructor that creates a queue with the specified size.
    *
    * @param capacity the size of the queue (cannot be changed)
    * @throws IllegalArgumentException if the size is &lt; 1
    */
  class CircularFifoQueue[E](val capacity: Int = 32) {
    if (capacity <= 0)
      throw new IllegalArgumentException("The size must be greater than 0")
    /** Underlying storage array. */
    private val elements = new Array[Any](capacity)
    /** Array index of first (oldest) queue element. */
    private var start = 0
    /**
      * Index mod maxElements of the array position following the last queue
      * element.  Queue elements start at elements[start] and "wrap around"
      * elements[maxElements-1], ending at elements[decrement(end)].
      * For example, elements = {c,a,b}, start=1, end=1 corresponds to
      * the queue [a,b,c].
      */
    private var end = 0
    /** Flag to indicate if the queue is currently full. */
    private var full = false
    /** Capacity of the queue. */
    final private val maxElements = elements.length

    /**
      * Constructor that creates a queue from the specified collection.
      * The collection size also sets the queue size.
      *
      * @param coll the collection to copy into the queue, may not be null
      * @throws NullPointerException if the collection is null
      */
    def this(coll: Traversable[_ <: E]) {
      this(coll.size)
      for (e <- coll) {
        this += e
      }
    }

    /**
      * Returns the number of elements stored in the queue.
      *
      * @return this queue's size
      */
    def size: Int = {
      var size = 0
      if (end < start) size = maxElements - start + end
      else if (end == start) size = if (full) maxElements
      else 0
      else size = end - start
      size
    }

    /**
      * Returns true if this queue is empty; false otherwise.
      *
      * @return true if this queue is empty
      */
    def isEmpty: Boolean = size == 0

    /**
      * Returns {@code true} if the capacity limit of this queue has been reached,
      * i.e. the number of elements stored in the queue equals its maximum size.
      *
      * @return { @code true} if the capacity limit has been reached, { @code false} otherwise
      * @since 4.1
      */
    def isFull: Boolean = size == maxElements

    /**
      * Gets the maximum size of the collection (the bound).
      *
      * @return the maximum number of elements the collection can hold
      */
    def maxSize: Int = maxElements

    /**
      * Clears this queue.
      */
    def clear(): Unit = {
      full = false
      start = 0
      end = 0
      for (i <- elements.indices) {
        elements(i) = null
      }
    }

    /**
      * Adds the given element to this queue. If the queue is full, the least recently added
      * element is discarded so that a new element can be inserted.
      *
      * @param element the element to add
      * @return true, always
      * @throws NullPointerException if the given element is null
      */
    def +=(element: E): Unit = {
      if (null == element) throw new NullPointerException("Attempted to add null object to queue")
      if (isFull) remove
      elements({
        end += 1
        end - 1
      }) = element.asInstanceOf[Any]
      if (end >= maxElements) end = 0
      if (end == start) full = true
    }

    def ++=(elements: Seq[E]): Unit = {
      if (elements.lengthCompare(capacity) >= 0) {
        val begin = elements.length - capacity
        for (i <- 0 until capacity) {
          this.elements(i) = elements(begin + i).asInstanceOf[Any]
          start = 0
          end = 0
          full = true
        }
      } else {
        for (e <- elements) {
          this += e
        }
      }
    }

    /**
      * Returns the element at the specified position in this queue.
      *
      * @param index the position of the element in the queue
      * @return the element at position { @code index}
      * @throws NoSuchElementException if the requested position is outside the range [0, size)
      */
    def apply(index: Int): E = {
      val sz = size
      if (index < 0 || index >= sz) throw new NoSuchElementException(s"The specified index $index is outside the available range [0, $sz)")
      val idx = (start + index) % maxElements
      elements(idx).asInstanceOf[E]
    }

    def update(index: Int, x: E): Unit = {
      val sz = size
      if (index < 0 || index >= sz) throw new NoSuchElementException(s"The specified index $index is outside the available range [0, $sz)")
      val idx = (start + index) % maxElements
      elements(idx) = x.asInstanceOf[Any]
    }

    def head: E = {
      if (isEmpty) throw new NoSuchElementException("queue is empty")
      elements(start).asInstanceOf[E]
    }

    def peek: E = head

    def remove: E = {
      if (isEmpty) throw new NoSuchElementException("queue is empty")
      val element = elements(start)
      if (null != element) {
        elements({
          start += 1
          start - 1
        }) = null
        if (start >= maxElements) start = 0
        full = false
      }
      element.asInstanceOf[E]
    }

    def last: E = {
      if (isEmpty) throw new NoSuchElementException("queue is empty")
      elements(end - 1).asInstanceOf[E]
    }

    def foreach(f: E => Unit): Unit = {
      for (e <- elements) {
        f(e.asInstanceOf[E])
      }
    }
  }

  class Boxed[T: ClassTag](var value: T) {
    def set(v: T): Unit = value = v

    def ==(v: T): Boolean = value == v

    def !=(v: T): Boolean = value != v

  }

  object Boxed {
    implicit def boxed2Value[T: ClassTag](boxed: Boxed[T]): T = boxed.value

    def apply[T: ClassTag](v: T): Boxed[T] = new Boxed(v)
  }


}
