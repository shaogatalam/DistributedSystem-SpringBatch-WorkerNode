package BasePack.Model;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
public class EmailObject implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private String email;
    private String template;
}
