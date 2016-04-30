package net.lazygun.experiment.reveno.demo;

import org.junit.Assert;
import org.reveno.atp.api.Reveno;
import org.reveno.atp.core.Engine;

import static org.reveno.atp.utils.MapUtils.map;

public class Demo {

    public static void main(String[] args) {
        new Demo().test();
    }

    private void test() {
        Reveno reveno = init("data/reveno-sample");
        reveno.startup();

        final long accountId = reveno.executeSync("createAccount", map("name", "John"));
        reveno.executeSync("changeBalance", map("id", accountId, "inc", 10_000));

        checkState(reveno, accountId);
        reveno.shutdown();

        reveno = init("data/reveno-sample");
        reveno.startup();

        checkState(reveno, accountId);
        reveno.shutdown();
    }

    private void checkState(Reveno reveno, long accountId) {
        Assert.assertNotNull(reveno.query().find(AccountView.class, accountId));
        Assert.assertEquals("John", reveno.query().find(AccountView.class, accountId).name);
        Assert.assertEquals(10_000, reveno.query().find(AccountView.class, accountId).balance);
    }

    private Reveno init(String folder) {
        final Reveno reveno = new Engine(folder);

        reveno.domain()
                .transaction("createAccount", (t, c) -> c.repo().store(t.id(), new Account(t.arg(), 0)))
                .uniqueIdFor(Account.class).command();

        reveno.domain()
                .transaction("changeBalance",
                             (t, c) -> c.repo().store(
                                     t.longArg(),
                                     c.repo().get(Account.class, t.arg()).add(t.intArg("inc"))
                             )).command();

        reveno.domain().viewMapper(Account.class, AccountView.class,
                                   (id, e, r) -> new AccountView(id, e.name, e.balance));
        return reveno;
    }
}
