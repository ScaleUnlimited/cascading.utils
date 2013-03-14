
### build.xml changes

A project using the datum compiler would add something like this:

```
    <target name="dc-init" depends="compile">
        <path id="datumcompiler.classpath">
            <path refid="compile.classpath" />
            <pathelement location="${build.dir.main-classes}" />
        </path>

        <taskdef name="dc" 
                 classname="com.scaleunlimited.cascading.DatumCompilerAntTask"
                 classpathref="datumcompiler.classpath" />
    </target>
```

And then you'd have one or more invocations of the "dc" task, for different datum templates.

The target directory would probably be something defined in build.properties, and included
in the list of things to compile. Which means the compile target would depend on datum-compile.

```
    <target name="datum-compile" depends="dc-init">
        <mkdir dir="build/generated" />

        <dc classname="my.package.MyDatumTemplate" srcDir="build/generated" />
    </target>
```

### Template class

The actual template class would be a source file (e.g. MyDatumTemplate.java) with just a list of fields, e.g.:

```
public class MyDatumTemplate {

    private String name;

    private String city;
    private String state;
    private String country;
    private int zipcode;
}
```

