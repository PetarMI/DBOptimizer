/**
 * 
 */
package sjdb;
import java.io.*;

/**
 * @author nmg
 *
 */
public class SJDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// read serialised catalogue from file and parse
		String catFile = args[0];
		Catalogue cat = new Catalogue();
		CatalogueParser catParser = new CatalogueParser(catFile, cat);
		catParser.parse();
		
		// read stdin, parse, and build canonical query plan
		QueryParser queryParser = new QueryParser(cat, new InputStreamReader(System.in));
		Operator plan = queryParser.parse();
		
		System.out.println(plan.toString());
		
		// create estimator visitor and apply it to canonical plan
		Estimator est = new Estimator();
		plan.accept(est);
		Relation output = plan.getOutput();
		System.out.println(output.getTupleCount());
		System.out.println(output.getAttributes());
		/*for (int i = 0; i < output.getAttributes().size(); i++) {
			System.out.println(output.getAttributes().get(i).getValueCount());
		}*/
		
		//create optimised plan
		Optimiser opt = new Optimiser(cat);
		Operator optPlan = opt.optimise(plan);
		
		System.out.println("\nOptimised plan");
		System.out.println(optPlan.toString());
		optPlan.accept(est);
		output = plan.getOutput();
		System.out.println(output.getTupleCount());
		System.out.println(output.getAttributes());
	}

}
