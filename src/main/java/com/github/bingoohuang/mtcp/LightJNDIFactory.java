package com.github.bingoohuang.mtcp;

import com.github.bingoohuang.mtcp.util.PropertyElf;

import javax.naming.*;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;

/**
 * A JNDI factory that produces LightDataSource instances.
 *
 * @author Brett Wooldridge
 */
public class LightJNDIFactory implements ObjectFactory {
    @Override
    synchronized public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {
        // We only know how to deal with <code>javax.naming.Reference</code> that specify a class name of "javax.sql.DataSource"
        if (obj instanceof Reference && "javax.sql.DataSource".equals(((Reference) obj).getClassName())) {
            Reference ref = (Reference) obj;
            Set<String> lightPropSet = PropertyElf.getPropertyNames(LightConfig.class);

            Properties properties = new Properties();
            Enumeration<RefAddr> enumeration = ref.getAll();
            while (enumeration.hasMoreElements()) {
                RefAddr element = enumeration.nextElement();
                String type = element.getType();
                if (type.startsWith("dataSource.") || lightPropSet.contains(type)) {
                    properties.setProperty(type, element.getContent().toString());
                }
            }
            return createDataSource(properties, nameCtx);
        }
        return null;
    }

    private DataSource createDataSource(final Properties properties, final Context context) throws NamingException {
        String jndiName = properties.getProperty("dataSourceJNDI");
        if (jndiName != null) {
            return lookupJndiDataSource(properties, context, jndiName);
        }

        return new LightDataSource(new LightConfig(properties));
    }

    private DataSource lookupJndiDataSource(final Properties properties, final Context context, final String jndiName) throws NamingException {
        if (context == null) {
            throw new RuntimeException("JNDI context does not found for dataSourceJNDI : " + jndiName);
        }

        DataSource jndiDS = (DataSource) context.lookup(jndiName);
        if (jndiDS == null) {
            final Context ic = new InitialContext();
            jndiDS = (DataSource) ic.lookup(jndiName);
            ic.close();
        }

        if (jndiDS != null) {
            LightConfig config = new LightConfig(properties);
            config.setDataSource(jndiDS);
            return new LightDataSource(config);
        }

        return null;
    }
}
