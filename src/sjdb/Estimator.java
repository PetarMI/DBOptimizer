package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());
		
		List<Attribute> attributesToRetain = op.getAttributes();
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			Attribute attr = iter.next();
			if (attributesToRetain.contains(attr)) {
				output.addAttribute(attr);
			}
		}
		
		op.setOutput(output);
	}
	
	public void visit(Select op) {
	}
	
	public void visit(Product op) {
		Relation leftRel = op.getLeft().getOutput();
		Relation rightRel = op.getRight().getOutput();
		
		int outputSize = leftRel.getTupleCount() * rightRel.getTupleCount();
		Relation output = new Relation(outputSize);
		
		Iterator<Attribute> leftIter = leftRel.getAttributes().iterator();
		while (leftIter.hasNext()) {
			output.addAttribute(new Attribute(leftIter.next()));
		}
		
		Iterator<Attribute> rightIter = rightRel.getAttributes().iterator();
		while (rightIter.hasNext()) {
			output.addAttribute(new Attribute(rightIter.next()));
		}
		
		op.setOutput(output);
	}
	
	public void visit(Join op) {
	}
}
