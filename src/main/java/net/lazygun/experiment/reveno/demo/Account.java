package net.lazygun.experiment.reveno.demo;

class Account {
    final String name;
    final long balance;

    Account(String name, long initialBalance) {
        this.name = name;
        this.balance = initialBalance;
    }

    Account add(long amount) {
        return new Account(name, balance + amount);
    }
}
