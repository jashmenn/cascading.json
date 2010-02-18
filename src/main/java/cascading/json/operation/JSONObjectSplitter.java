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
 * JSONObjectSplitter : Given a Tuple with one field being a String of a
 * JSONObject, emit a Tuple for every (key, value) pair.
 *
 * @author <a href="mailto:nmurray@attinteractive.com">Nate Murray</a>
 *
 */
public class JSONObjectSplitter extends JSONOperation implements Function {

  public JSONObjectSplitter(Fields fieldDeclaration){
    super( fieldDeclaration );
  }

  public void operate(FlowProcess flowProcess, FunctionCall functionCall){
    String in = functionCall.getArguments().get(0).toString();
    JSONObject jsonObject = JSONObject.fromObject(in);
  
    if(jsonObject.size() > 0) {
      Iterator it = jsonObject.entrySet().iterator();
      while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          functionCall.getOutputCollector().add( new Tuple((String)pair.getKey(), (String)pair.getValue()) );
      }
    } else {
      functionCall.getOutputCollector().add( new Tuple(null, null) );
    }

  }
}
