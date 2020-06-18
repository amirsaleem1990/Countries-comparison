public class Test{
	public static void main(String[] args){
		brother bro = new brother();
		String brother_name = bro.name;
		int brother_age = bro.age;
		bro.print_();

		father fath = new father();
		String father_name = fath.name;
		int father_age = fath.age;
		fath.print_();

		System.out.println("My Father's name is: " + father_name +  ", and  he is " + father_age + " old\nMy brother's name is: " + brother_name +  ", and he is " + brother_age + " old\n");

	}
}