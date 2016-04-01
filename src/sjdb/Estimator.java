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
		Relation input = op.getInput().getOutput();
		Predicate predicate = op.getPredicate();
		Attribute leftAttr = input.getAttribute(predicate.getLeftAttribute());
		
		if (predicate.equalsValue()) {
			int size = input.getTupleCount() / leftAttr.getValueCount();
			Relation output = new Relation(size);
			
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attr = iter.next();
				if (!attr.equals(leftAttr)) {
					output.addAttribute(new Attribute(attr));
				}
				else {
					output.addAttribute(new Attribute(attr.getName(), 1));
				}
			}
			
			op.setOutput(output);
		}
		else {
			Attribute rightAttr = input.getAttribute(predicate.getRightAttribute());
			int size = input.getTupleCount() / Math.max(leftAttr.getValueCount(), rightAttr.getValueCount());
			Relation output = new Relation(size);
			
			int valueCount = Math.min(leftAttr.getValueCount(), rightAttr.getValueCount());
			
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attr = iter.next();
				if (!attr.equals(leftAttr) && !attr.equals(rightAttr)) {
					output.addAttribute(new Attribute(attr));
				}
				else {
					output.addAttribute(new Attribute(attr.getName(), valueCount));
				}
			}
			
			op.setOutput(output);
		}
		
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
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();
		Attribute leftAttr = leftInput.getAttribute(op.getPredicate().getLeftAttribute());
		Attribute rightAttr = rightInput.getAttribute(op.getPredicate().getRightAttribute());
		
		int size = (leftInput.getTupleCount() * rightInput.getTupleCount()) /
				Math.max(leftAttr.getValueCount(), rightAttr.getValueCount());
		Relation output = new Relation(size);
		
		int attrValueCount = Math.min(leftAttr.getValueCount(), rightAttr.getValueCount());
		
		addRelationAttributes(leftInput, output, leftAttr, attrValueCount);
		addRelationAttributes(rightInput, output, rightAttr, attrValueCount);
		
		op.setOutput(output);
	}
	
	private void addRelationAttributes(Relation input, Relation output, Attribute joinAttr, int valueCount) {
		Iterator<Attribute> iter = input.getAttributes().iterator();
		
		while (iter.hasNext()) {
			Attribute attr = iter.next();
			if (!attr.equals(joinAttr)) {
				output.addAttribute(new Attribute(attr));
			}
			else {
				output.addAttribute(new Attribute(attr.getName(), valueCount));
			}
		}
	}
}
