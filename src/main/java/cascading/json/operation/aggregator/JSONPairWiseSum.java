package cascading.json.operation.aggregator;

import java.util.Iterator;

import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import cascading.flow.FlowProcess;
import cascading.json.JSONUtils;
import cascading.json.JSONWritable;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.TupleEntry;

/**
 * JSONPairWiseSum: 
 *    calculate pair-wise sums using JSON striped records
 *
 * {@code}
 *
 * input:  (key, 
 *            ({a:1}),
 *            ({a:1}),
 *            ({b:3)) 
 * output: (key, {a:2,b3}) 
 *
 * {code}
 * 
 * @author <a href="mailto:nate@natemurray.com">Nate Murray</a>
 */

public class JSONPairWiseSum extends BaseOperation<Object> implements Aggregator<Object>
  {
    private static final String DEFAULT_FORMAT = "JSONArray";
    // private String format;

  /**
   * @param fieldDeclaration of type Fields
   */
  public JSONPairWiseSum( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  /**
   * @param fieldDeclaration of type Fields
   */
  public JSONPairWiseSum( Fields fieldDeclaration )
    {
      this( fieldDeclaration, DEFAULT_FORMAT ); 
    }

  public void start( FlowProcess flowProcess, AggregatorCall<Object> aggregatorCall )
    {
      Object context = new JSONObject();
      aggregatorCall.setContext( context );
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<Object> aggregatorCall )
    {
      JSONObject context = (JSONObject)aggregatorCall.getContext();
      TupleEntry arguments = aggregatorCall.getArguments();
      Tuple tuple = arguments.getTuple();
      JSONObject v = JSONObject.fromObject(tuple.getString(0));

      for(Map.Entry<String,Integer> entry : (Set<Map.Entry<String,Integer>>)v.entrySet() )
      {
        String otherKeyId = entry.getKey();
        Integer otherCount = entry.getValue();

        Integer newCount = otherCount;
        if(context.containsKey(otherKeyId)) {
          newCount += (Integer)context.get(otherKeyId);
        }
        context.put(otherKeyId, newCount);
      }
    }

  public void complete( FlowProcess flowProcess, AggregatorCall<Object> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  private Tuple getResult( AggregatorCall<Object> aggregatorCall )
    {
    Object context = aggregatorCall.getContext();
    return new Tuple( context.toString() );
    }
  }
