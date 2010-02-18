/*
 * Copyright 2009, 
 */

package cascading.json.operation;

import net.sf.json.JSON;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import cascading.flow.FlowProcess;
import cascading.json.JSONUtils;
import cascading.json.JSONWritable;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import java.util.Iterator;
import java.util.Map;

/**
 * JSONRegexSplitStripeConverter 
 * Given a Tuple with one field being a String, split that String on Regex.
 * 
 * emit a JSONObject with for each token with a count of 1 (as a Stripe)
 *
 * {@code}
 *    input:
 *    (a, "foo,bar,baz") 
 * 
 *    emits:
 *    (a, '{"foo":"1","bar":"1","baz":"1"}')
 * 
 * {code}
 
 * @author <a href="mailto:nmurray@attinteractive.com">Nate Murray</a>
 *
 */
public class JSONRegexSplitStripeConverter extends JSONOperation implements Function {
  /** Field patternString */
  private String patternString;

  /** Field pattern */
  private transient Pattern pattern;

  public JSONRegexSplitStripeConverter(Fields fieldDeclaration, String patternString){
    super( fieldDeclaration );
    this.patternString = patternString;
  }

  public void operate(FlowProcess flowProcess, FunctionCall functionCall){
    String value = functionCall.getArguments().getString( 0 );

    if( value == null )
      value = "";

    JSONObject jsonObject = new JSONObject();
    String[] split = getPattern().split(value);

    for( int i = 0; i < split.length; i++ )
      jsonObject.put(split[i], new Integer(1));

    functionCall.getOutputCollector().add( new Tuple(jsonObject.toString()) );
  }

  /**
   * Method getPattern returns the pattern of this RegexOperation object.
   *
   * @return the pattern (type Pattern) of this RegexOperation object.
   */
  protected Pattern getPattern()
  {
    if( pattern != null )
      return pattern;

    pattern = Pattern.compile( patternString );

    return pattern;
  }

}
