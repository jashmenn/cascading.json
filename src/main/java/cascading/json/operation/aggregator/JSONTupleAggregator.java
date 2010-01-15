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
 * JSONTupleAggregator: 
 *    groups input tuples into a json string as either a JSONArray or JSONObject e.g.
 *
 * {@code}
 *    input:
 *    (a,          <- group
 *      (foo, bar),
 *      (bam, baz))
 * 
 *    emits:
 *    (a, '["foo","bar"],["bam","baz"]')
 *    or:
 *    (a, '{"foo":"bar","bam":"baz"}')
 * 
 * {code}
 * 
 * Note that using a JSONObject will clobber any repeat keys. However using
 * JSONArray will keep duplicates.
 *
 * @author <a href="mailto:nate@natemurray.com">Nate Murray</a>
 */

public class JSONTupleAggregator extends BaseOperation<Object> implements Aggregator<Object>
  {
    private static final String DEFAULT_FORMAT = "JSONArray";
    private String format;

  /**
   * @param fieldDeclaration of type Fields
   * @param format String denoting the format of the JSON. Either "JSONArray" or "JSONObject"
   */
  public JSONTupleAggregator( Fields fieldDeclaration, String format )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    this.format = format;
    }

  /**
   * @param fieldDeclaration of type Fields
   */
  public JSONTupleAggregator( Fields fieldDeclaration )
    {
      this( fieldDeclaration, DEFAULT_FORMAT ); 
    }

  public void start( FlowProcess flowProcess, AggregatorCall<Object> aggregatorCall )
    {
      Object context = this.format.equals("JSONObject") ? new JSONObject() : new JSONArray(); 
      aggregatorCall.setContext( context );
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<Object> aggregatorCall )
    {
      if(this.format.equals("JSONObject")) {
        JSONObject context = (JSONObject)aggregatorCall.getContext();
        TupleEntry arguments = aggregatorCall.getArguments();
        Tuple tuple = arguments.getTuple();
        context.put(tuple.getString(0), tuple.getString(1));
      } else {
        JSONArray context = (JSONArray)aggregatorCall.getContext();
        TupleEntry arguments = aggregatorCall.getArguments();
        Tuple tuple = arguments.getTuple();
        JSONArray tupie = new JSONArray();
        for ( int i=0; i< tuple.size(); i++ ) {
          tupie.element( tuple.getString( i ) );
        }
        context.element( JSONArray.toCollection( tupie ) );
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
