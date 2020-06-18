public class class_with_cunstructer{

	String name = "Amir";
	int age = 30;

	// Constructor
	public class_with_cunstructer(){
		name = "Oqba";
		age = 2; 
	}
	// onother way to define Constructor is to defice it with perameters, to jab ham apni main me is class ka object banaen gay tabhi us ko perameters dy den gy

	public void print_func(){
		System.out.println("\nclass with cunstructer\nName = " + name + "\nAge = " + age);
	}
}