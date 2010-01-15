package cascading.json.operation;

import cascading.CascadingTestCase;
import org.junit.* ;
import static org.junit.Assert.* ;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;
import cascading.json.operation.*;
import java.util.Iterator;

public class JSONObjectSplitterTestCase extends CascadingTestCase {
  public JSONObjectSplitterTestCase()
    {
    super("JSONObjectSplitter test");
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    }

   @Test
   public void test_objectsplitter() {
      System.out.println("Test that JSONObjectSplitter...");

      JSONObjectSplitter operation = new JSONObjectSplitter(new Fields(0,1));

      Tuple next;
      TupleListCollector tlc = invokeFunction(operation, new Tuple("{'foo':'bar','bing':'baz'}"), new Fields(0,1));
      Iterator it = tlc.iterator();
      assertEquals("tlc should have 2", 2, tlc.size());
      next = (Tuple)it.next();
      assertEquals("not equal: next.get(0)", "foo", next.get(0));
      assertEquals("not equal: next.get(1)", "bar", next.get(1));
      next = (Tuple)it.next();
      assertEquals("not equal: next.get(0)", "bing", next.get(0));
      assertEquals("not equal: next.get(1)", "baz", next.get(1));
   }
}

