package cascading.flow.planner;


public class NamingFlowStep {

    // Provide access to protected methods

    public static void setName(BaseFlowStep<Object> flowStep, String name) {
        // Avoid triggering exception by Cascading
        if ((name != null) && (name.length() > 0)) {
            flowStep.setName(name);
        }
    }
    
   
}
