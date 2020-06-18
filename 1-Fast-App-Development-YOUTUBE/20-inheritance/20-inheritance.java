// jab ham ksi dusri file me mojud koi class apny main me use karna chahty hen to hamary pas 2 options hen:
// 1- us class ka object bana kar us ka contant use karen, magar is me masla ye h k jab ham us class ka object banaen gy to us ka sara contanc memory me load ho jay ga, or agar wo class bhi bari hwi to kafi memory consume ho jay gi jis ka koi faida bhi nahi q k hame to us class me sy srif kuch hi functions ka kaam h
// 2- us class ko inherent kar len, inherent karny k lye ham pehli line me ye words add kar den gy <extends parent_class_name>, phir main me apni hi class ka object banaen gy, or phir us object k zary apni class or parent class dono ka content access karen gy 

public class Test extends parent {
	public static void main(String[] args){
		// jis class me abhi hen usi ka object bana rahy hen
		Test t = new Test();
		// jo class <parent> extend ki thi us ka function yahan use kar rahy hen
		t.print_(); 
		// apni hi class ka function using object use kar rahy hen
		t.SHOW();
	}
	public static void SHOW() {
		System.out.println("<SHOW> function called Successfully");
	}
}