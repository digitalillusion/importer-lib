package xyz.deverse.importer;

public enum ActionType {
    DELETE(1),
    DETACH(1),
    PERSIST(1),
    IGNORE(0);

    private int order;

    ActionType(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }
}
