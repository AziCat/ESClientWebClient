/**
 *
 */
package cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc;

import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;

import org.springframework.jdbc.core.PreparedStatementSetter;

/**
 * @author huangjiajun
 *
 */
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.ParameterDisposer;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.LobCreator;


/**
 * 大字段保存到数据库的PreparedStatementSetter
 */
public class LobCreatorPreparedStatementSetter implements
		PreparedStatementSetter, ParameterDisposer {

	private final Object[] args;
	private final int[] argTypes;
	private final LobCreator lobCreator;

	/**
	 * Create a new ArgTypePreparedStatementSetter for the given arguments.
	 *
	 * @param args
	 *            the arguments to set
	 * @param argTypes
	 *            the corresponding SQL types of the arguments
	 * @param lobCreator
	 */
	public LobCreatorPreparedStatementSetter(Object[] args, int[] argTypes,
			LobCreator lobCreator) {
		if ((args != null && argTypes == null)
				|| (args == null && argTypes != null)
				|| (args != null && args.length != argTypes.length)) {
			throw new InvalidDataAccessApiUsageException(
					"args and argTypes parameters must match");
		}
		this.args = args;
		this.argTypes = argTypes;
		this.lobCreator = lobCreator;
	}

	public void setValues(PreparedStatement ps) throws SQLException {
		int argIndx = 1;
		if (this.args != null) {
			for (int i = 0; i < this.args.length; i++) {
				Object arg = this.args[i];
				if (arg instanceof Collection
						&& this.argTypes[i] != Types.ARRAY) {
					Collection entries = (Collection) arg;
					for (Iterator it = entries.iterator(); it.hasNext();) {
						Object entry = it.next();
						if (entry instanceof Object[]) {
							Object[] valueArray = ((Object[]) entry);
							for (int k = 0; k < valueArray.length; k++) {
								Object argValue = valueArray[k];
								StatementCreatorUtils.setParameterValue(ps,
										argIndx++, new SqlParameter(this.argTypes[i]), argValue);
							}
						} else {
							StatementCreatorUtils.setParameterValue(ps,
									argIndx++, new SqlParameter(this.argTypes[i]), entry);
						}
					}
				} else if (arg instanceof byte[]
						&& (this.argTypes[i] == Types.BLOB || this.argTypes[i] == Types.BINARY)) {
					// 是大字段的,用这种方式
					lobCreator.setBlobAsBytes(ps, argIndx++, (byte[]) arg);
				} else if (arg instanceof String
						&& this.argTypes[i] == Types.CLOB) {
					lobCreator.setClobAsCharacterStream(ps, argIndx++,
							new StringReader((String) arg), ((String) arg)
									.length());
				} else if (arg instanceof String
						&& this.argTypes[i] == Types.VARCHAR
						&& ((String) arg).length() > 2000) {
					// 这里是针对oracle做的特殊处理,oracle字符串大于2000时,需要这样搞
					ps.setCharacterStream(argIndx++, new StringReader(
							(String) arg), ((String) arg).length());
				} else {
					StatementCreatorUtils.setParameterValue(ps, argIndx++,
							new SqlParameter(this.argTypes[i]), arg);
				}
			}
		}
	}

	public void cleanupParameters() {
		StatementCreatorUtils.cleanupParameters(this.args);
	}
}