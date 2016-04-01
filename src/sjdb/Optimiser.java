package sjdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Optimiser {
	
	private Catalogue catalogue;
	private Estimator estimator;
	private Operator revisedPlan; 
	private List<Select> selectOps;
	
	public Optimiser(Catalogue cat) {
		this.catalogue = cat;
		this.estimator = new Estimator();
		this.revisedPlan = null;
		this.selectOps = new ArrayList<Select>();
	}
	
	public Operator optimise(Operator canonicalPlan) {
		this.revisedPlan = this.copyCanonicalPlan(canonicalPlan);
		this.revisedPlan = this.moveSelects(revisedPlan);
		this.selectOps.clear();
		//this.revisedPlan = this.makeJoins(revisedPlan);
		if (this.performMoveProjects(revisedPlan)) {
			this.revisedPlan = this.moveProjects(revisedPlan, new ArrayList<Attribute>());	
		}
		
		return this.revisedPlan;
	}
	
	/**
	 * Main SELECT moving function
	 */
	private Operator moveSelects(Operator plan) {
		if (plan instanceof Scan) {
			List<Select> selects = getScanSelects(plan, this.selectOps);
			
			Operator revisedPlan = plan;
			
			for (Select select : selects) {
				revisedPlan = new Select(revisedPlan, select.getPredicate());
			}
			
			return revisedPlan;
		} 
		else if (plan instanceof Select) {
			Select opCast = (Select) plan;
			this.selectOps.add(opCast);
			
			Operator revisedPlan = moveSelects(opCast.getInput());
			
			//select wasn't moved down
			if (!this.selectOps.contains(opCast)) {
				return revisedPlan;
			}
			//the select has been moved down
			else {
				return new Select(revisedPlan, opCast.getPredicate());
			}
		}
		else if (plan instanceof Product) {
			Product opCast = (Product) plan;
			Operator revisedLeftPlan = moveSelects(opCast.getLeft());
			Operator revisedRightPlan = moveSelects(opCast.getRight());
			Operator revisedPlan = new Product(revisedLeftPlan, revisedRightPlan);
			
			//if there is a chance to move selects get the product outputs
			if (!this.selectOps.isEmpty()) {
				revisedPlan.accept(estimator);
			}
			
			List<Select> selects = getProductSelects((Product)revisedPlan, this.selectOps);
			
			for (Select select : selects) {
				revisedPlan = new Select(revisedPlan, select.getPredicate());
			}
			
			return revisedPlan;
		}
		//operator is project
		else {
			Project opCast = (Project) plan;
			
			Operator revisedPlan = moveSelects(opCast.getInput());
			
			return new Project (revisedPlan, opCast.getAttributes());
		}
	}
	
	/**
	 * Main JOIN creating function
	 */
	private Operator makeJoins(Operator plan) {
		if (plan instanceof Project) {
			Project opCast = (Project) plan;
			
			Operator revisedPlan = makeJoins(opCast.getInput());
			return new Project(revisedPlan, opCast.getAttributes());
		}
		else if (plan instanceof Select) {
			Select opCast = (Select) plan;
			Operator revisedPlan;
			
			if (!opCast.getPredicate().equalsValue()) {
				selectOps.add(opCast);
				revisedPlan = makeJoins(opCast.getInput());
				
				//the select hasn't been combined into a JOIN
				if (this.selectOps.contains(opCast)) {
					return new Select(revisedPlan, opCast.getPredicate());
				}
				//the select has been used for creating a JOIN
				else {
					return revisedPlan;
				}
			}
			else {
				revisedPlan = makeJoins(opCast.getInput());
				return new Select(revisedPlan, opCast.getPredicate());
			}
			
		}
		else if (plan instanceof Product) {
			Product opCast = (Product) plan;
			Predicate predicate = getJoinPredicate(opCast, this.selectOps);
			
			Operator leftRevised = makeJoins(opCast.getLeft());
			Operator rightRevised = makeJoins(opCast.getRight());
			
			if (predicate != null) {
				return new Join(leftRevised, rightRevised, predicate);
			}
			else {
				return new Product(leftRevised, rightRevised);
			}
			
		}
		//it is a ...
		else {
			return plan;
		}
	}
	
	/**
	 * Main PROJECT moving function
	 */
	private Operator moveProjects (Operator plan, List<Attribute> parentAttrs) {
		if (plan instanceof Project) {
			Project opCast = (Project) plan;
			parentAttrs.addAll(opCast.getAttributes());
			
			Operator revisedPlan = moveProjects(opCast.getInput(), parentAttrs);
			
			return revisedPlan;
		}
		else if (plan instanceof Select) {
			Select opCast = (Select) plan;
			List<Attribute> curLevelAttrs = this.addPredicateAttributes(opCast, parentAttrs);
			
			Operator revisedPlan = moveProjects(opCast.getInput(), curLevelAttrs);
			revisedPlan = new Select(revisedPlan, opCast.getPredicate());
			revisedPlan.accept(this.estimator);
			
			return appendProject(revisedPlan, parentAttrs);
			
		}
		/*else if (plan instanceof Join) {
			
		}*/
		else if (plan instanceof Product) {
			Product opCast = (Product) plan;
			Operator leftRevised = moveProjects(opCast.getLeft(), parentAttrs);
			Operator rightRevised = moveProjects(opCast.getRight(), parentAttrs);
			
			Operator revisedPlan = new Product(leftRevised, rightRevised);
			revisedPlan.accept(this.estimator);
			
			return appendProject(revisedPlan, parentAttrs);
		}
		//it is a scan
		else {
			Scan opCast = (Scan) plan;
			return appendProject(opCast, parentAttrs);
		}
	}
	
	/**
	 * Create an identical plan to the one passed for optimisation
	 */
	private Operator copyCanonicalPlan(Operator canonicalPlan) {
		if (canonicalPlan instanceof Select) {
			Select opCast = (Select) canonicalPlan;
			Predicate predicate = opCast.getPredicate();
			
			Operator copyInput = copyCanonicalPlan(opCast.getInput());
			
			return new Select(copyInput, predicate);
		} 
		else if (canonicalPlan instanceof Project) {
			Project opCast = (Project) canonicalPlan;
			List<Attribute> attributes = opCast.getAttributes();
			
			Operator copyInput = copyCanonicalPlan(opCast.getInput());
			
			return new Project(copyInput, attributes);
		}
		else if (canonicalPlan instanceof Product) {
			Operator leftInput = canonicalPlan.getInputs().get(0);
			Operator rightInput = canonicalPlan.getInputs().get(1);
			
			leftInput = copyCanonicalPlan(leftInput);
			rightInput = copyCanonicalPlan(rightInput);
			
			return new Product(leftInput, rightInput);
		}
		//only option left is a Scan
		else {
			Scan opCast = (Scan) canonicalPlan;
			NamedRelation relation = (NamedRelation) opCast.getRelation();
			
			return new Scan(relation);
		}
	} 
	
	/**
	 * Private helper functions for SELECT moving
	 * getScanSelects
	 * isSelectValid (Scan)
	 * isSelectValid (Product)
	 * getProductSelects
	 */
	private List<Select> getScanSelects (Operator op, List<Select> selects) {
		Scan scan = (Scan) op;
		List<Select> validSelects = new ArrayList<Select>();
		
		Iterator<Select> iter = selects.iterator();
		while (iter.hasNext()) {
			Select select = iter.next();
			if (isSelectValid(scan, select)) {
				validSelects.add(select);
				iter.remove();
			}
		}
		
		return validSelects;
	}
	
	private boolean isSelectValid(Scan scan, Select select) {
		List<Attribute> relationAttrs = scan.getRelation().getAttributes();
		Predicate predicate = select.getPredicate();

		if (predicate.equalsValue()) {
			if (relationAttrs.contains(predicate.getLeftAttribute())) {
				return true;
			}
			else {
				return false;
			}
		}
		else {
			return false;
		}
	}
	
	private List<Select> getProductSelects(Product product, List<Select> selects) {
		List<Select> validSelects = new ArrayList<Select>();
		
		Iterator<Select> iter = selects.iterator();
		while (iter.hasNext()) {
			Select select = iter.next();
			if (isSelectValid(product, select)) {
				validSelects.add(select);
				iter.remove();
			}
		}
		
		return validSelects;
	}
	
	private boolean isSelectValid(Product product, Select select) {
		Predicate predicate = select.getPredicate();
		List<Attribute> relationAttrs = product.getOutput().getAttributes();
		List<Attribute> predicateAttrs = new ArrayList<Attribute>();
		predicateAttrs.add(predicate.getLeftAttribute());
		
		if (!predicate.equalsValue()) {
			predicateAttrs.add(predicate.getRightAttribute());
		}
		
		return relationAttrs.containsAll(predicateAttrs);
	}
	
	private Predicate getJoinPredicate(Product product, List<Select> selects) {
		List<Select> validSelects = getProductSelects(product, selects);
		Predicate predicate = null;
		
		//Sanity check
		if (validSelects.size() > 1) {
			System.out.println("The number of selects for a given cartesian product is larger than one");
		}
		
		//get a select to use for the join at random
		if (!validSelects.isEmpty()) {
			predicate = validSelects.get(0).getPredicate();
			
			//Sanity check
			if (predicate.equalsValue()) {
				System.out.println("A Valid predicate for a given cartesian product equals a value");
			}
		}
		
		return predicate;
	}
	
	/**
	 * Private helper functions for PROJECT moving
	 */
	/**
	 * Takes the attribute list passed from the upper level and returns a new list
	 * containing parent attributes and attributes from the current SELECT
	 * @param op - the current operator  
	 * @param parentAttrs - the original attribute list passed from the top level
	 */
	private List<Attribute> addPredicateAttributes(Select op, List<Attribute> parentAttrs) {
		List<Attribute> allAttrs = new ArrayList<Attribute>(parentAttrs); 
		Predicate predicate = op.getPredicate();
		allAttrs.add(predicate.getLeftAttribute());
		
		if (!predicate.equalsValue()) {
			allAttrs.add(predicate.getRightAttribute());
		}
		
		return allAttrs;
	}
	
	/**
	 * Takes the attribute list passed from the upper level and returns a new list
	 * containing parent attributes and attributes from the current SELECT
	 * @param op - the current operator  
	 * @param parentAttrs - the original attribute list passed from the top level
	 */
	private List<Attribute> addPredicateAttributes(Join op, List<Attribute> parentAttrs) {
		List<Attribute> allAttrs = new ArrayList<Attribute>(parentAttrs);
		Predicate predicate = op.getPredicate();
		
		allAttrs.add(predicate.getLeftAttribute());
		allAttrs.add(predicate.getRightAttribute());
		
		return allAttrs;
	}
	
	/**
	 * Decided whether to put a new PROJECT on top of the current tree
	 * @param revisedPlan - Input plan with estimated output relations
	 * @param curLevelAttrs - All attributes that are needed up the tree and at this level
	 * @return
	 */
	private Operator appendProject(Operator revisedPlan, List<Attribute> curLevelAttrs) {
		Relation relation = revisedPlan.getOutput();
		List<Attribute> projAttrs = getProjectAttributes(curLevelAttrs, relation.getAttributes());
		
		//All attributes are needed further up
		if (projAttrs.size() == 0 || projAttrs.size() == relation.getAttributes().size()) {
			return revisedPlan;
		}
		else {
			return new Project(revisedPlan, projAttrs);
		}
	}
	
	/**
	 * Get only the required attributes from the relation
	 * @param relAttrs
	 * @return
	 */
	private List<Attribute> getProjectAttributes(List<Attribute> allAttrs, List<Attribute> relAttrs) {
		List<Attribute> projAttrs = new ArrayList<Attribute>();
		
		for (Attribute attr : relAttrs) {
			if (allAttrs.contains(attr)) {
				projAttrs.add(attr);
			}
		}
		
		return projAttrs;
	}
	
	/**
	 * Decide if we should perform Projection moving
	 * @param plan - the plan that we are working on
	 * @return - true or false
	 */
	private boolean performMoveProjects(Operator plan) {
		if (plan instanceof Project) {
			return true;
		}
		else {
			return false;
		}
	}
}
